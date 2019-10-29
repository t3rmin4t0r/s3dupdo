package org.notmysock.repl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Works {

  static final MetricRegistry metricRegistry = new MetricRegistry();
  static final String PROCESSED_FILES = "processedFiles";
  static final Meter processedFiles = metricRegistry.meter(PROCESSED_FILES);

  interface Work {
    void execute(Configuration conf) throws Exception;

    default void report() {
      ConsoleReporter reporter = ConsoleReporter
          .forRegistry(metricRegistry)
          .convertRatesTo(TimeUnit.SECONDS)
          .build();
      reporter.report();
    }
  }

  static Connection getConnection(String name) throws SQLException {
    java.nio.file.Path currentRelativePath = Paths.get("");
    String cwd = currentRelativePath.toAbsolutePath().toString();
    final String statedb = "jdbc:sqlite:" + cwd + "/" + name;
    Connection conn = DriverManager.getConnection(statedb);
    return conn;
  }

  public static class PlanWork implements Work {

    private static final Logger LOG = LoggerFactory.getLogger(PlanWork.class
        .getName());

    private final String dst;
    private final String src;
    private final String name;
    private final int numNodes;

    public PlanWork(String name, String src, String dst, int numNodes)
        throws URISyntaxException {
      this.name = name;
      this.src = src;
      // Add "/" if required. e.g s3a://bucket/dst ->  s3a://bucket/dst/
      if (!dst.endsWith("/")) {
        dst = dst + "/";
      }
      this.dst = dst;
      this.numNodes = numNodes;
    }

    private Connection createDB() throws SQLException {
      try {
        Connection conn = getConnection(name);
        if (conn != null) {
          Statement stmt = conn.createStatement();
          String ctas = "CREATE TABLE IF NOT EXISTS FILES" +
              " (SRC TEXT NOT NULL PRIMARY KEY " +
              ", DST TEXT NOT NULL " +
              ", SIZE INT NOT NULL " +
              ", NODE_ID INT NOT NULL " +
              ", COPIED BOOLEAN)";
          stmt.executeUpdate(ctas);
          stmt.close();
        }
        return conn;
      } catch (SQLException e) {
        System.out.println(e.getMessage());
        throw e;
      }
    }

    @Override
    public void execute(Configuration conf) throws Exception {
      try (Connection db = createDB(); FileSystem fs = FileSystem.get(new URI(src), conf)) {
        db.setAutoCommit(false);
        PreparedStatement insert = db.prepareStatement("INSERT INTO FILES VALUES(?, ?, ?, ?, ?)");

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new org.apache.hadoop.fs.Path(src), true);
        int nodeIndex = 0;
        while (listFiles.hasNext()) {
          LocatedFileStatus lf = listFiles.next();
          String from = lf.getPath().toString();
          String to = (lf.getPath().toString().replace(src.toString(), dst.toString()));
          long size = lf.getLen();
          insert.setString(1, from);
          insert.setString(2, to);
          insert.setLong(3, size);
          insert.setInt(4, nodeIndex++);
          insert.setBoolean(5, false);
          insert.executeUpdate();
          nodeIndex = nodeIndex % numNodes;
          processedFiles.mark();
        }
        db.commit();
      }
    }

    @Override
    public String toString() {
      return String.format("Plan(%s, %s -> %s)", name, src, dst);
    }
  }

  /**
   * To provide details about files to be copied over.
   */
  public static class InfoWork implements Work {

    private final String name;

    public InfoWork(String name) {
      this.name = name;
    }

    @Override
    public void execute(Configuration conf) throws Exception {
      String stats = "select COUNT(1) as c, SUM(SIZE) as sz,"
          + " COUNT(DISTINCT NODE_ID) distinctNodes, COPIED "
          + "from FILES GROUP BY COPIED";

      try(Connection conn = getConnection(name);
          Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery(stats)) {

        while (rs.next()) {
          String copiedStatus = rs.getString(4);
          if (copiedStatus.equalsIgnoreCase("0")) {
            System.out.println("Copied files stats:");
          } else {
            System.out.println("Yet to be copied files stats:");
          }

          System.out.println("\tTotal files : " + rs.getString(1));
          System.out.println("\tTotal size : " + rs.getString(2));
          System.out.println("\tDistinct Nodes : " + rs.getString(3));

          System.out.println();
        }
      }
    }
  }

  public static class CopyWork implements Work {

    private final String name;
    private final int parallel;
    private long numFiles = -1;
    private long totalSize = -1;
    private final int nodeId;

    public CopyWork(String name, int parallel, int nodeId) {
      this.name = name;
      this.parallel = parallel;
      this.nodeId = nodeId;
    }

    @Override
    public void execute(final Configuration conf) throws Exception {
      try (Connection db = getConnection(name)) {
        Statement stmt = db.createStatement();
        String stats = "select COUNT(1) as c, SUM(SIZE) as sz from FILES ";
        if (nodeId != -1) {
         stats = stats +" WHERE NODE_ID=" + nodeId;
        }
        ResultSet rs = stmt.executeQuery(stats);
        while (rs.next()) {
          numFiles = rs.getInt("c");
          totalSize = rs.getLong("sz");
        }
        rs.close();
        stmt.close();

        if (numFiles == 0) {
          System.out.println("No files to copy.");
          return;
        }

        stmt = db.createStatement();
        String inputs =
            "select SRC, DST, SIZE from FILES where copied <> 1 ";
        if (nodeId != -1) {
          inputs = inputs + " AND NODE_ID=" + nodeId;
        }
        inputs = inputs + " ORDER BY SIZE";

        rs = stmt.executeQuery(inputs);
        int maxItems = (int) (numFiles + 1);
        ArrayBlockingQueue<CopyOp> copies = new ArrayBlockingQueue<>(maxItems);
        while (rs.next()) {
          copies.offer(new CopyOp(rs.getString(1), rs.getString(2), rs
              .getLong(3)));
        }
        rs.close();
        stmt.close();

        if (copies.isEmpty()) {
          System.out.println("All files are already copied as per database. "
              + "If you still need to recopy, run `plan` again.");
          return;
        }

        ExecutorService pool = Executors.newFixedThreadPool(parallel);
        Future<?>[] results = new Future<?>[parallel];
        for (int i = 0; i < parallel; i++) {
          results[i] = pool.submit(new CopyWorker(conf, copies, db, nodeId));
        }
        for (int i = 0; i < parallel; i++) {
          results[i].get();
        }
        pool.shutdown();
      }
    }

    @Override
    public String toString() {
      return String.format("CopyWork(%s)", name);
    }
  }

  public static class CopyOp {
    public final String src;
    public final String dst;
    public final long size;

    public CopyOp(String src, String dst, long size) {
      this.src = src;
      this.dst = dst;
      if (src.equals(dst)) {
        throw new IllegalArgumentException(
            String.format("Source cannot be the same as destination: %s", src));
      }
      this.size = size;
    }

    @Override
    public String toString() {
      return String.format("%s -(%d)-> %s", src, size, dst);
    }
  }

  public static class CopyWorker implements Callable<Boolean> {
    private static final Logger LOG = LoggerFactory.getLogger(CopyWorker.class
        .getName());

    final ArrayBlockingQueue<CopyOp> queue;
    final Configuration conf;
    final Connection db;
    final int nodeId;
    static final AtomicInteger counter = new AtomicInteger(0);

    public CopyWorker(Configuration conf, ArrayBlockingQueue<CopyOp> copies,
        Connection db, int nodeId) {
      this.queue = copies;
      this.conf = conf;
      this.db = db;
      this.nodeId = nodeId;
    }

    private void complete(CopyOp op) throws IOException {
      System.out.println("Pending copies : " + queue.size());
      synchronized (db) {
        try {
          PreparedStatement update = db
              .prepareStatement("UPDATE FILES SET COPIED=1 WHERE SRC=?");
          update.setString(1, op.src);
          update.executeUpdate();
          update.close();
        } catch (SQLException e) {
          e.printStackTrace();
          throw new IOException(e);
        }
      }
    }

    public void run() {
      CopyOp op = this.queue.poll();
      Thread.currentThread().setName(
          String.format("CopyWorker-%02d", counter.getAndIncrement()));
      final FileSystem srcFS;
      final FileSystem dstFS;
      try {
        srcFS = FileSystem.newInstance(new URI(op.src), conf);
        dstFS = FileSystem.newInstance(new URI(op.dst), conf);
      } catch (IOException e) {
        e.printStackTrace();
        return;
      } catch (URISyntaxException e) {
        e.printStackTrace();
        return;
      }

      do {
        try {
          System.out.println("Opening " + op.src + "-->" + op.dst);
          FSDataInputStream in = srcFS.open(new Path(op.src));
          FSDataOutputStream out = dstFS.create(new Path(op.dst), true);
          IOUtils.copyBytes(in, out, 1024 * 1024);
          in.close();
          out.close();
          LOG.info("Copied (nodeId={}) {}", nodeId, op);
          // everything looks okay 
          complete(op);
          processedFiles.mark();
          op = this.queue.poll();
          if (op == null) {
            break;
          }
        } catch (IllegalArgumentException e) {
          e.printStackTrace();
          break;
        } catch (IOException e) {
          e.printStackTrace();
          break;
        }
      } while (true);
    }

    @Override
    public Boolean call() throws Exception {
      run();
      return true;
    }
  }

}
