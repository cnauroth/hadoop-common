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
  private final MetricMutableCounterLong numberOfFilesCreated =
      registry.newCounter(
          "asv_files_created",
          "Total number of files created through the ASV file system.",
          0L);
  private final MetricMutableGaugeLong bytesWrittenInLastSecond =
      registry.newGauge(
          "asv_bytes_written_last_second",
          "Total number of bytes written to Azure Storage during the last second.",
          0L);

  /**
   * Indicate that we just got a web response from Azure Storage. This should
   * be called for every web request/response we do (to get accurate metrics
   * of how we're hitting the storage service).
   */
  public void webResponse() {
    numberOfWebResponses.incr();
  }

  /**
   * Indicate that we just created a file through ASV.
   */
  public void fileCreated() {
    numberOfFilesCreated.incr();
  }

  /**
   * Sets the current gauge value for how many bytes were written in the last
   *  second.
   * @param currentBytesWritten The number of bytes.
   */
  public void updateBytesWrittenInLastSecond(long currentBytesWritten) {
    bytesWrittenInLastSecond.set(currentBytesWritten);
  }

  @Override
  public void getMetrics(MetricsBuilder builder, boolean all) {
    registry.snapshot(builder.addRecord(registry.name()), all);
  }
}
