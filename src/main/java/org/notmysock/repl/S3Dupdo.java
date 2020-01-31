package org.notmysock.repl;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.notmysock.repl.Works.Work;

public class S3Dupdo extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new S3Dupdo(), args);
    System.exit(res);
  }

  private static enum Operation {
    PLAN,
    RUN,
    INFO,
    VERIFY,
    RESET;

    public static Operation getOperation(String optionValue) {
      return Operation.valueOf(optionValue.toUpperCase());
    }
  }

  static Options options;
  static {
    options = new org.apache.commons.cli.Options();
    options.addOption("op", "operation", true, "operation (plan, run, verify)");
    options.addOption("p", "parallel", true, "parallelize n-way");
    options.addOption("n", "nodes", true, "parallelize n-nodes");
    options.addOption("i", "nodeId", true, "nodeId during copy");
    options.addOption("s", "src", true, "source data");
    options.addOption("d", "dst", true, "destination data");
    options.addOption("v", "verbose", true, "verbose");
    options.addOption("awsKey", "awsKey", true, "AWS accesskey:secret");
  }

  private static void help(String error) {
    HelpFormatter f = new HelpFormatter();
    f.printHelp(S3Dupdo.class.getSimpleName(), error, options, "", true);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String[] remainingArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();

    CommandLineParser parser = new BasicParser();
    CommandLine line = parser.parse(options, remainingArgs);

    final Operation op;
    if (!(line.hasOption("operation"))) {
      help("Missing operation");
      return 1;
    } else {
      op = Operation.getOperation(line.getOptionValue("operation"));
    }

    if (line.hasOption("awsKey")) {
      String awsKey = line.getOptionValue("awsKey");
      String[] ks = awsKey.split(":", 2);
      conf.set("fs.s3a.access.key", ks[0]);
      conf.set("fs.s3a.secret.key", ks[1]);
    }

    if (line.getArgList().size() == 0) {
      help("Provide DB file name where it should be persisted to.");
      return 1;
    }

    final String name = (String) line.getArgList().get(0);

    final Work work;

    final int parallel;
    // Set nodeId to -1 if unspecified. This node would pull all files.
    int nodeId = (line.hasOption("nodeId")) ?
        Integer.parseInt(line.getOptionValue("nodeId")) : -1;
    if (nodeId != -1) {
      System.out.println("Using nodeId: " + nodeId);
    }
    if (!line.hasOption("parallel")) {
      parallel = 1;
    } else {
      parallel = Integer.parseInt(line.getOptionValue("parallel"));
    }

    switch (op) {
    case PLAN:
      if (!line.hasOption("src")) {
        help("Missing src for planning");
        return 1;
      }
      if (!line.hasOption("dst")) {
        help("Missing dst for planning");
        return 1;
      }
      int numNodes = 1;
      if (line.hasOption("nodes")) {
        numNodes = Integer.parseInt(line.getOptionValue("nodes"));
        System.out.println("Using numNodes: " + numNodes);
      }
      work = new Works.PlanWork(name, line.getOptionValue("src"),
          line.getOptionValue("dst"), numNodes);
      break;
    case RUN:
      work = new Works.CopyWork(name, parallel, nodeId);
      break;
    case INFO:
      work = new Works.InfoWork(name);
      break;
    case RESET:
      // TODO
      work = null;
      break;
    case VERIFY:
      // TODO
      work = new Works.VerifyWork(name, parallel);
      break;
    default:
      work = null;
      break;
    }

    if (work != null) {
      work.execute(getConf());
      work.report();
    }

    return 0;
  }
}