package org.apache.hadoop.fs.azurenative;

import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.metrics2.lib.*;

/**
 * A metrics source for the ASV file system to track all the metrics we care
 * about for getting a clear picture of the performance/reliability/interaction
 * of the Hadoop cluster with Azure Storage.  
 */
final class AzureFileSystemInstrumentation implements MetricsSource {
  private final MetricsRegistry registry =
      new MetricsRegistry("azureFileSystem");
  private final MetricMutableCounterLong numberOfWebResponses =
      registry.newCounter(
          "asv_web_responses",
          "Total number of web responses obtained from Azure Storage",
          0L);
  
  /**
   * Indicate that we just got a web response from Azure Storage. This should
   * be called for every web request/response we do (to get accurate metrics
   * of how we're hitting the storage service).
   */
  public void webResponse() {
    numberOfWebResponses.incr();
  }
  
  @Override
  public void getMetrics(MetricsBuilder builder, boolean all) {
    registry.snapshot(builder.addRecord(registry.name()), all);
  }
}
