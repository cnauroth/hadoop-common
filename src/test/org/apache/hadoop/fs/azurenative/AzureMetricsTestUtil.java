package org.apache.hadoop.fs.azurenative;

import static org.apache.hadoop.test.MetricsAsserts.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.mockito.ArgumentCaptor;

public final class AzureMetricsTestUtil {
  public static final String ASV_WEB_RESPONSES = "asv_web_responses";
  public static final String ASV_BYTES_WRITTEN =
      "asv_bytes_written_last_second";
  public static final String ASV_BYTES_READ =
      "asv_bytes_read_last_second";
  public static final String ASV_RAW_BYTES_UPLOADED =
      "asv_raw_bytes_uploaded";
  public static final String ASV_RAW_BYTES_DOWNLOADED =
      "asv_raw_bytes_downloaded";
  public static final String ASV_FILES_CREATED = "asv_files_created";
  public static final String ASV_FILES_DELETED = "asv_files_deleted";
  public static final String ASV_DIRECTORIES_CREATED = "asv_directories_created";
  public static final String ASV_DIRECTORIES_DELETED = "asv_directories_deleted";
  public static final String ASV_UPLOAD_RATE =
      "asv_maximum_upload_bytes_per_second";
  public static final String ASV_DOWNLOAD_RATE =
      "asv_maximum_download_bytes_per_second";

  public static long getLongGaugeValue(AzureFileSystemInstrumentation instrumentation,
      String gaugeName) {
    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(getMetrics(instrumentation))
      .addGauge(eq(gaugeName), anyString(),
        captor.capture());
    return captor.getValue();
  }

  /**
   * Gets the current value of the given counter.
   */
  public static long getLongCounterValue(AzureFileSystemInstrumentation instrumentation,
      String counterName) {
    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(getMetrics(instrumentation))
      .addCounter(eq(counterName), anyString(),
        captor.capture());
    return captor.getValue();
  }

  /**
   * Gets the current value of the asv_bytes_written_last_second counter.
   */
  public static long getCurrentBytesWritten(AzureFileSystemInstrumentation instrumentation) {
    return getLongGaugeValue(instrumentation, ASV_BYTES_WRITTEN);
  }

  /**
   * Gets the current value of the asv_bytes_read_last_second counter.
   */
  public static long getCurrentBytesRead(AzureFileSystemInstrumentation instrumentation) {
    return getLongGaugeValue(instrumentation, ASV_BYTES_READ);
  }

  /**
   * Gets the current value of the asv_raw_bytes_uploaded counter.
   */
  public static long getCurrentTotalBytesWritten(
      AzureFileSystemInstrumentation instrumentation) {
    return getLongCounterValue(instrumentation, ASV_RAW_BYTES_UPLOADED);
  }

  /**
   * Gets the current value of the asv_raw_bytes_downloaded counter.
   */
  public static long getCurrentTotalBytesRead(
      AzureFileSystemInstrumentation instrumentation) {
    return getLongCounterValue(instrumentation, ASV_RAW_BYTES_DOWNLOADED);
  }
}
