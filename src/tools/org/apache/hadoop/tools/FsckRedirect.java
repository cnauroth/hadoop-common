package org.apache.hadoop.tools;

import java.net.URI;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.azurenative.*;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.util.*;

/**
 * A thin Fsck redirector that redirects fsck calls to either
 * AsvFsck or DFsck depending on what the default file system is.
 */
public class FsckRedirect extends Configured implements Tool {
  public FsckRedirect(Configuration conf) {
    super(conf);
  }

  @Override
  public int run(String[] args) throws Exception {
    String path = "/";
    for (String currentArg : args) {
      if (!currentArg.startsWith("-")) {
        // Path given on the command-line
        path = currentArg;
        break;
      }
    }
    // Check if the file system we're checking is ASV.
    FileSystem fs = FileSystem.get(new URI(path), getConf());
    if (fs instanceof NativeAzureFileSystem) {
      // Run the ASV file-check
      return new AsvFsck(getConf()).run(args);
    } else {
      // Just fall back on DFS
      return new DFSck(getConf()).run(args);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new FsckRedirect(new Configuration()), args);
    System.exit(res);
  }
}
