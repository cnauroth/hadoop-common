package org.apache.hadoop.fs.azurenative;

import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.metrics2.lib.*;

/**
 * A metrics source for the ASV file system to track all the metrics we care
 * about for getting a clear picture of the performance/reliability/interaction
 * of the Hadoop cluster with Azure Storage.  
 */
final class AzureFileSystemInstrumentation implements MetricsSource {
  static final String ASV_WEB_RESPONSES = "asv_web_responses";
  static final String ASV_BYTES_WRITTEN =
      "asv_bytes_written_last_second";
  static final String ASV_BYTES_READ =
      "asv_bytes_read_last_second";
  static final String ASV_RAW_BYTES_UPLOADED =
      "asv_raw_bytes_uploaded";
  static final String ASV_RAW_BYTES_DOWNLOADED =
      "asv_raw_bytes_downloaded";
  static final String ASV_FILES_CREATED = "asv_files_created";
  static final String ASV_FILES_DELETED = "asv_files_deleted";
  static final String ASV_DIRECTORIES_CREATED = "asv_directories_created";
  static final String ASV_DIRECTORIES_DELETED = "asv_directories_deleted";
  static final String ASV_UPLOAD_RATE =
      "asv_maximum_upload_bytes_per_second";
  static final String ASV_DOWNLOAD_RATE =
      "asv_maximum_download_bytes_per_second";
  static final String ASV_UPLOAD_LATENCY =
      "asv_average_block_upload_latency_ms";
  static final String ASV_DOWNLOAD_LATENCY =
      "asv_average_block_download_latency_ms";
  static final String ASV_CLIENT_ERRORS = "asv_client_errors";
  static final String ASV_SERVER_ERRORS = "asv_server_errors";

  /**
   * Config key for how big the rolling window size for latency metrics should
   * be (in seconds).
   */
  private static final String KEY_ROLLING_WINDOW_SIZE = "fs.azure.metrics.rolling.window.size";

  private final MetricsRegistry registry =
      new MetricsRegistry("azureFileSystem")
      .setContext("azureFileSystem");
  private final MetricMutableCounterLong numberOfWebResponses =
      registry.newCounter(
          ASV_WEB_RESPONSES,
          "Total number of web responses obtained from Azure Storage",
          0L);
  private final MetricMutableCounterLong numberOfFilesCreated =
      registry.newCounter(
          ASV_FILES_CREATED,
          "Total number of files created through the ASV file system.",
          0L);
  private final MetricMutableCounterLong numberOfFilesDeleted =
      registry.newCounter(
          ASV_FILES_DELETED,
          "Total number of files deleted through the ASV file system.",
          0L);
  private final MetricMutableCounterLong numberOfDirectoriesCreated =
      registry.newCounter(
          ASV_DIRECTORIES_CREATED,
          "Total number of directories created through the ASV file system.",
          0L);
  private final MetricMutableCounterLong numberOfDirectoriesDeleted =
      registry.newCounter(
          ASV_DIRECTORIES_DELETED,
          "Total number of directories deleted through the ASV file system.",
          0L);
  private final MetricMutableGaugeLong bytesWrittenInLastSecond =
      registry.newGauge(
          ASV_BYTES_WRITTEN,
          "Total number of bytes written to Azure Storage during the last second.",
          0L);
  private final MetricMutableGaugeLong bytesReadInLastSecond =
      registry.newGauge(
          ASV_BYTES_READ,
          "Total number of bytes read from Azure Storage during the last second.",
          0L);
  private final MetricMutableGaugeLong maximumUploadBytesPerSecond =
      registry.newGauge(
          ASV_UPLOAD_RATE,
          "The maximum upload rate encountered to Azure Storage in bytes/second.",
          0L);
  private final MetricMutableGaugeLong maximumDownloadBytesPerSecond =
      registry.newGauge(
          ASV_DOWNLOAD_RATE,
          "The maximum download rate encountered to Azure Storage in bytes/second.",
          0L);
  private final MetricMutableCounterLong rawBytesUploaded =
      registry.newCounter(
          ASV_RAW_BYTES_UPLOADED,
          "Total number of raw bytes (including overhead) uploaded to Azure" +
          " Storage.",
          0L);
  private final MetricMutableCounterLong rawBytesDownloaded =
      registry.newCounter(
          ASV_RAW_BYTES_DOWNLOADED,
          "Total number of raw bytes (including overhead) downloaded from Azure" +
          " Storage.",
          0L);
  private final MetricMutableCounterLong clientErrors =
      registry.newCounter(
          ASV_CLIENT_ERRORS,
          "Total number of client-side errors by ASV (excluding 404).",
          0L);
  private final MetricMutableCounterLong serverErrors =
      registry.newCounter(
          ASV_SERVER_ERRORS,
          "Total number of server-caused errors by ASV.",
          0L);
  private final MetricMutableGaugeLong averageBlockUploadLatencyMs;
  private final MetricMutableGaugeLong averageBlockDownloadLatencyMs;
  private long currentMaximumUploadBytesPerSecond;
  private long currentMaximumDownloadBytesPerSecond;
  private static final int DEFAULT_LATENCY_ROLLING_AVERAGE_WINDOW =
      5; // seconds
  private final RollingWindowAverage currentBlockUploadLatency;
  private final RollingWindowAverage currentBlockDownloadLatency;
  private UUID fileSystemInstanceId;

  public AzureFileSystemInstrumentation(Configuration conf) {
    fileSystemInstanceId = UUID.randomUUID();
    registry.tag("asvFileSystemId",
        "A unique identifier for the file ",
        fileSystemInstanceId.toString());
    final int rollingWindowSizeInSeconds =
        conf.getInt(KEY_ROLLING_WINDOW_SIZE,
            DEFAULT_LATENCY_ROLLING_AVERAGE_WINDOW);
    averageBlockUploadLatencyMs =
        registry.newGauge(
            ASV_UPLOAD_LATENCY,
            String.format("The average latency in milliseconds of uploading a single block" +
            ". The average latency is calculated over a %d-second rolling" +
            " window.", rollingWindowSizeInSeconds),
            0L);
    averageBlockDownloadLatencyMs =
        registry.newGauge(
            ASV_DOWNLOAD_LATENCY,
            String.format("The average latency in milliseconds of downloading a single block" +
            ". The average latency is calculated over a %d-second rolling" +
            " window.", rollingWindowSizeInSeconds),
            0L);
    currentBlockUploadLatency =
        new RollingWindowAverage(rollingWindowSizeInSeconds * 1000);
    currentBlockDownloadLatency =
        new RollingWindowAverage(rollingWindowSizeInSeconds * 1000);
  }

  /**
   * The unique identifier for this file system in the metrics.
   */
  public UUID getFileSystemInstanceId() {
    return fileSystemInstanceId;
  }

  /**
   * Sets the account name to tag all the metrics with.
   * @param accountName The account name.
   */
  public void setAccountName(String accountName) {
    registry.tag("accountName",
        "Name of the Azure Storage account that these metrics are going against",
        accountName);
  }

  /**
   * Sets the container name to tag all the metrics with.
   * @param containerName The container name.
   */
  public void setContainerName(String containerName) {
    registry.tag("containerName",
        "Name of the Azure Storage container that these metrics are going against",
        containerName);
  }

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

  /**
   * Indicate that we just uploaded a block and record its latency.
   * @param latency The latency in milliseconds.
   */
  public void blockUploaded(long latency) {
    currentBlockUploadLatency.addPoint(latency);
  }

  /**
   * Indicate that we just downloaded a block and record its latency.
   * @param latency The latency in milliseconds.
   */
  public void blockDownloaded(long latency) {
    currentBlockDownloadLatency.addPoint(latency);
  }

  /**
   * Indicate that we just encountered a client-side error.
   */
  public void clientErrorEncountered() {
    clientErrors.incr();
  }

  /**
   * Indicate that we just encountered a server-caused error.
   */
  public void serverErrorEncountered() {
    serverErrors.incr();
  }

  @Override
  public void getMetrics(MetricsBuilder builder, boolean all) {
    averageBlockDownloadLatencyMs.set(
        currentBlockDownloadLatency.getCurrentAverage());
    averageBlockUploadLatencyMs.set(
        currentBlockUploadLatency.getCurrentAverage());
    registry.snapshot(builder.addRecord(registry.name()), all);
  }
}
