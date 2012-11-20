package org.apache.hadoop.fs.azurenative;

import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.metrics2.lib.*;

final class AzureFileSystemInstrumentation implements MetricsSource {
  private final MetricsRegistry registry =
      new MetricsRegistry("azureFileSystem");
  private final MetricMutableCounterLong numberOfWebRequests =
      registry.newCounter(
          "asv_web_requests",
          "Total number of web requests made to Azure Storage",
          0L);
  
  public void webRequest() {
    numberOfWebRequests.incr();
  }
  
  @Override
  public void getMetrics(MetricsBuilder builder, boolean all) {
    registry.snapshot(builder.addRecord(registry.name()), all);
  }
}
