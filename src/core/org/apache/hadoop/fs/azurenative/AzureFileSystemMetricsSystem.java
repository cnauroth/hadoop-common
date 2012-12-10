package org.apache.hadoop.fs.azurenative;

import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;

final class AzureFileSystemMetricsSystem {
  private static MetricsSystemImpl instance;
  private static int numFileSystems;
  
  public synchronized static void fileSystemStarted() {
    if (numFileSystems == 0) {
      instance = new MetricsSystemImpl();
      instance.init("azure-file-system");
      instance.start();
    }
    numFileSystems++;
  }
  
  public synchronized static void fileSystemClosed() {
    instance.publishMetricsNow();
    if (numFileSystems == 1) {
      instance.stop();
      instance.shutdown();
      instance = null;
    }
    numFileSystems--;
  }
  
  public static void registerSource(String name, String desc,
      MetricsSource source) {
    instance.register(name, desc, source);
  }
}
