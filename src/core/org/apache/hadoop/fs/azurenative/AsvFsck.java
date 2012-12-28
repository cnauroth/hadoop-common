package org.apache.hadoop.fs.azurenative;

import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;

public class AsvFsck extends Configured implements Tool {
  public AsvFsck(Configuration conf) {
    super(conf);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (doPrintUsage(Arrays.asList(args))) {
      printUsage();
      return -1;
    }
    Path pathToCheck = null;
    boolean doRecover = false;
    for (String arg : args) {
      if (!arg.startsWith("-")) {
        if (pathToCheck != null) {
          System.err.println(
              "Can't specify multiple paths to check on the command-line");
          return 1;
        }
        pathToCheck = new Path(arg);
      } else if (arg.equals("-recover")) {
        doRecover = true;
      }
    }
    if (pathToCheck == null) {
      pathToCheck = new Path("/"); // Check everything.
    }
    FileSystem fs = FileSystem.get(pathToCheck.toUri(), getConf());
    if (!(fs instanceof NativeAzureFileSystem)) {
      System.err.println("Can only check ASV file system. Instead I'm asked to" +
          " check: " + fs.getUri());
      return 2;
    }
    NativeAzureFileSystem asvFs = (NativeAzureFileSystem)fs;
    if (doRecover) {
      asvFs.recoverFilesWithDanglingTempData(pathToCheck);
    }
    return 0;
  }

  private static void printUsage() {
    System.out.println("Usage: AsvFSck [<path>] [-recover]");
    System.out.println("\t<path>\tstart checking from this path");
    System.out.println("\t-recover\trecover any files whose upload was interrupted mid-stream.");
    ToolRunner.printGenericCommandUsage(System.out);
  }

  private boolean doPrintUsage(List<String> args) {
    return args.contains("-?") || args.contains("-H");
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new AsvFsck(new Configuration()), args);
    System.exit(res);
  }
}
