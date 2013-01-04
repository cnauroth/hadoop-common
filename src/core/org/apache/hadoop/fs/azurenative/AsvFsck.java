package org.apache.hadoop.fs.azurenative;

import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;

/**
 * An fsck tool implementation for ASV that does various admin/cleanup/recovery
 * tasks on the ASV file system.
 */
public class AsvFsck extends Configured implements Tool {
  private FileSystem mockFileSystemForTesting = null;
  private static final String lostAndFound = "/lost+found";

  public AsvFsck(Configuration conf) {
    super(conf);
  }

  /**
   * For testing purposes, set the file system to use here instead of
   * relying on getting it from the FileSystem class based on the URI.
   * @param fileSystem The file system to use.
   */
  public void setMockFileSystemForTesting(FileSystem fileSystem) {
    this.mockFileSystemForTesting = fileSystem;
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
      } else if (arg.equals("-move")) {
        doRecover = true;
      }
    }
    if (pathToCheck == null) {
      pathToCheck = new Path("/"); // Check everything.
    }
    FileSystem fs;
    if (mockFileSystemForTesting == null) {
      fs = FileSystem.get(pathToCheck.toUri(), getConf());
    } else {
      fs = mockFileSystemForTesting;
    }
    if (!(fs instanceof NativeAzureFileSystem)) {
      System.err.println("Can only check ASV file system. Instead I'm asked to" +
          " check: " + fs.getUri());
      return 2;
    }
    NativeAzureFileSystem asvFs = (NativeAzureFileSystem)fs;
    if (doRecover) {
      asvFs.recoverFilesWithDanglingTempData(pathToCheck, new Path(lostAndFound));
    }
    return 0;
  }

  private static void printUsage() {
    System.out.println("Usage: AsvFSck [<path>] [-move]");
    System.out.println("\t<path>\tstart checking from this path");
    System.out.println("\t-move\tmove any files whose upload was interrupted" +
    		" mid-stream to " + lostAndFound);
    ToolRunner.printGenericCommandUsage(System.out);
  }

  private boolean doPrintUsage(List<String> args) {
    return args.contains("-H");
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new AsvFsck(new Configuration()), args);
    System.exit(res);
  }
}
