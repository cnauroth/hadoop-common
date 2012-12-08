package org.apache.hadoop.fs.azurenative;

import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;

final class AzureFileSystemMetricsSystem {
  private static final MetricsSystemImpl instance =
      new MetricsSystemImpl();
  private static int numFileSystems;
  private static boolean initialized = false;
  
  public synchronized static void fileSystemStarted() {
    if (numFileSystems == 0) {
      if (!initialized) {
        instance.init("azure-file-system");
        initialized = true;
      }
      instance.start();
    }
    numFileSystems++;
  }
  
  public synchronized static void fileSystemClosed() {
    instance.publishMetricsNow();
    if (numFileSystems == 1) {
      instance.stop();
      instance.shutdown();
      initialized = false;
    }
    numFileSystems--;
  }
  
  public static void registerSource(String name, String desc,
      MetricsSource source) {
    instance.register(name, desc, source);
  }
}
