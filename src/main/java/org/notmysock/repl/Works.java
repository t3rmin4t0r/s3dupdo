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

import org.apache.commons.logging.LogFactory;
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

  public static interface Work {
    public void execute(Configuration conf) throws Exception;
  }

  public static class PlanWork implements Work {
    private final String dst;
    private final String src;
    private final String name;

    public PlanWork(String name, String src, String dst) throws URISyntaxException {
      this.name = name;
      this.src = src;
      if (dst.endsWith("/")) {
        dst = dst.substring(0, dst.length()-1);
      }
      this.dst = dst;
    }
    
    private Connection createDB() throws SQLException {
      java.nio.file.Path currentRelativePath = Paths.get("");
      String cwd = currentRelativePath.toAbsolutePath().toString();
      final String statedb = "jdbc:sqlite:" + cwd + "/" + name + ".sqlite";
      try {
        Connection conn = DriverManager.getConnection(statedb);
        if (conn  != null) {
          Statement stmt = conn.createStatement();
          String ctas = "CREATE TABLE IF NOT EXISTS FILES" +
          " (SRC TEXT NOT NULL PRIMARY KEY " +
          ", DST TEXT NOT NULL " +
          ", SIZE INT NOT NULL " +
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
      
      Connection db = createDB();
      db.setAutoCommit(false);
      PreparedStatement insert = db.prepareStatement("INSERT INTO FILES VALUES(?, ?, ?, ?)");
      FileSystem fs = FileSystem.get(new URI(src), conf);
      try {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new org.apache.hadoop.fs.Path(src), true);
        while (listFiles.hasNext()) {
          LocatedFileStatus lf = listFiles.next();
          String from = lf.getPath().toString();
          String to = (lf.getPath().toString().replace(src.toString(), dst.toString()));
          long size = lf.getLen();
          insert.setString(1, from);
          insert.setString(2, to);
          insert.setLong(3, size);
          insert.setBoolean(4, false);
          insert.executeUpdate();
        }
        db.commit();
      } finally {
        if(fs != null) {
          fs.close();
        }
        if (db != null) {
          db.close();
        }
      }
    }


    @Override
    public String toString() {
      return String.format("Plan(%s, %s -> %s)", name, src, dst);
    }
  }
  

  public static class CopyWork implements Work {
    
    private final String name;
    private final int parallel;
    private long numFiles = -1;
    private long totalSize = -1;
    
    public CopyWork(String name, int parallel) {
      this.name = name;
      this.parallel = parallel;
    }
    
    private Connection createDB() throws SQLException {
      java.nio.file.Path currentRelativePath = Paths.get("");
      String cwd = currentRelativePath.toAbsolutePath().toString();
      final String statedb = "jdbc:sqlite:" + cwd + "/" + name + ".sqlite";
      try {
        Connection conn = DriverManager.getConnection(statedb);
        return conn;
      } catch (SQLException e) {
        System.out.println(e.getMessage());
        throw e;
      }
    }

    @Override
    public void execute(final Configuration conf) throws Exception {
      Connection db = createDB();

      try {
        Statement stmt = db.createStatement();
        String stats = "select COUNT(1) as c, SUM(SIZE) as sz from FILES";
        ResultSet rs  = stmt.executeQuery(stats);
        while (rs.next()) {
          numFiles = rs.getInt("c");
          totalSize = rs.getLong("sz");
        }
        rs.close();
        stmt.close();
        
        stmt = db.createStatement();
        String inputs = "select SRC, DST, SIZE from FILES where copied <> 1 ORDER BY SIZE";
        rs = stmt.executeQuery(inputs);
        int maxItems =  (int) (numFiles + 1);
        ArrayBlockingQueue<CopyOp> copies = new ArrayBlockingQueue<>(maxItems);
        while (rs.next()) {
          copies.offer(new CopyOp(rs.getString(1), rs.getString(2), rs
              .getLong(3)));
        }
        rs.close();
        stmt.close();
        ExecutorService pool = Executors.newFixedThreadPool(parallel);
        Future<?>[] results = new Future<?>[parallel];
        for (int i = 0; i < parallel; i++) {
          results[i] = pool.submit(new CopyWorker(conf, copies, db));
        }
        for (int i = 0; i < parallel; i++) {
          results[i].get();
        }
        pool.shutdown();
      } finally {
        db.close();
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
    static final AtomicInteger counter = new AtomicInteger(0);
    
    public CopyWorker(Configuration conf, ArrayBlockingQueue<CopyOp> copies, Connection db) {
      this.queue = copies;
      this.conf = conf;
      this.db = db;
    }

    private void complete(CopyOp op) throws IOException   {
      System.out.println(Thread.currentThread().getName() + " :" + op );
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
          FSDataInputStream in = srcFS.open(new Path(op.src));
          FSDataOutputStream out = dstFS.create(new Path(op.dst), true);
          IOUtils.copyBytes(in, out, 1024*1024);
          in.close();
          out.close();
          LOG.info("Copied {}", op);
          // everything looks okay 
          complete(op);
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
      } while(true);
    }

    @Override
    public Boolean call() throws Exception {
      run();
      return true;
    }
  }


}
