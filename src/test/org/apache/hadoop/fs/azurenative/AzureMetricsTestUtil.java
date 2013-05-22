package org.apache.hadoop.fs.azurenative;

import static org.apache.hadoop.test.MetricsAsserts.*;
import static org.apache.hadoop.fs.azurenative.AzureFileSystemInstrumentation.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.mockito.ArgumentCaptor;

public final class AzureMetricsTestUtil {
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
