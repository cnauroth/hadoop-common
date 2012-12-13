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
  private final MetricMutableCounterLong numberOfFilesDeleted =
      registry.newCounter(
          "asv_files_deleted",
          "Total number of files deleted through the ASV file system.",
          0L);
  private final MetricMutableCounterLong numberOfDirectoriesCreated =
      registry.newCounter(
          "asv_directories_created",
          "Total number of directories created through the ASV file system.",
          0L);
  private final MetricMutableCounterLong numberOfDirectoriesDeleted =
      registry.newCounter(
          "asv_directories_deleted",
          "Total number of directories deleted through the ASV file system.",
          0L);
  private final MetricMutableGaugeLong bytesWrittenInLastSecond =
      registry.newGauge(
          "asv_bytes_written_last_second",
          "Total number of bytes written to Azure Storage during the last second.",
          0L);
  private final MetricMutableGaugeLong bytesReadInLastSecond =
      registry.newGauge(
          "asv_bytes_read_last_second",
          "Total number of bytes read from Azure Storage during the last second.",
          0L);
  private final MetricMutableGaugeLong maximumUploadBytesPerSecond =
      registry.newGauge(
          "asv_maximum_upload_bytes_per_second",
          "The maximum upload rate encountered to Azure Storage in bytes/second.",
          0L);
  private final MetricMutableGaugeLong maximumDownloadBytesPerSecond =
      registry.newGauge(
          "asv_maximum_download_bytes_per_second",
          "The maximum download rate encountered to Azure Storage in bytes/second.",
          0L);
  private final MetricMutableCounterLong rawBytesUploaded =
      registry.newCounter(
          "asv_raw_bytes_uploaded",
          "Total number of raw bytes (including overhead) uploaded to Azure" +
          " Storage.",
          0L);
  private final MetricMutableCounterLong rawBytesDownloaded =
      registry.newCounter(
          "asv_raw_bytes_downloaded",
          "Total number of raw bytes (including overhead) downloaded from Azure" +
          " Storage.",
          0L);
  private long currentMaximumUploadBytesPerSecond;
  private long currentMaximumDownloadBytesPerSecond;

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
   * Indicate that we just deleted a file through ASV.
   */
  public void fileDeleted() {
    numberOfFilesDeleted.incr();
  }

  /**
   * Indicate that we just created a directory through ASV.
   */
  public void directoryCreated() {
    numberOfDirectoriesCreated.incr();
  }

  /**
   * Indicate that we just deleted a directory through ASV.
   */
  public void directoryDeleted() {
    numberOfDirectoriesDeleted.incr();
  }

  /**
   * Sets the current gauge value for how many bytes were written in the last
   *  second.
   * @param currentBytesWritten The number of bytes.
   */
  public void updateBytesWrittenInLastSecond(long currentBytesWritten) {
    bytesWrittenInLastSecond.set(currentBytesWritten);
  }

  /**
   * Sets the current gauge value for how many bytes were read in the last
   *  second.
   * @param currentBytesRead The number of bytes.
   */
  public void updateBytesReadInLastSecond(long currentBytesRead) {
    bytesReadInLastSecond.set(currentBytesRead);
  }

  /**
   * Record the current bytes-per-second upload rate seen.
   * @param bytesPerSecond The bytes per second.
   */
  public synchronized void currentUploadBytesPerSecond(long bytesPerSecond) {
    if (bytesPerSecond > currentMaximumUploadBytesPerSecond) {
      currentMaximumUploadBytesPerSecond = bytesPerSecond;
      maximumUploadBytesPerSecond.set(bytesPerSecond);
    }
  }

  /**
   * Record the current bytes-per-second download rate seen.
   * @param bytesPerSecond The bytes per second.
   */
  public synchronized void currentDownloadBytesPerSecond(long bytesPerSecond) {
    if (bytesPerSecond > currentMaximumDownloadBytesPerSecond) {
      currentMaximumDownloadBytesPerSecond = bytesPerSecond;
      maximumDownloadBytesPerSecond.set(bytesPerSecond);
    }
  }

  /**
   * Indicate that we just uploaded some data to Azure storage.
   * @param numberOfBytes The raw number of bytes uploaded (including overhead).
   */
  public void rawBytesUploaded(long numberOfBytes) {
    rawBytesUploaded.incr(numberOfBytes);
  }

  /**
   * Indicate that we just downloaded some data to Azure storage.
   * @param numberOfBytes The raw number of bytes downloaded (including overhead).
   */
  public void rawBytesDownloaded(long numberOfBytes) {
    rawBytesDownloaded.incr(numberOfBytes);
  }

  @Override
  public void getMetrics(MetricsBuilder builder, boolean all) {
    registry.snapshot(builder.addRecord(registry.name()), all);
  }
}
