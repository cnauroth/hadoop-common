package org.apache.hadoop.fs.azurenative;

import static org.apache.hadoop.test.MetricsAsserts.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.mockito.ArgumentCaptor;

public final class AzureMetricsTestUtil {
  public static final String ASV_WEB_RESPONSES = "asv_web_responses";
  public static final String ASV_BYTES_WRITTEN = "asv_bytes_written_last_second";
  public static final String ASV_RAW_BYTES_UPLOADED =
      "asv_raw_bytes_uploaded";

  /**
   * Gets the current value of the asv_web_responses counter.
   */
  public static long getCurrentBytesWritten(AzureFileSystemInstrumentation instrumentation) {
    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(getMetrics(instrumentation))
      .addGauge(eq(ASV_BYTES_WRITTEN), anyString(),
        captor.capture());
    return captor.getValue();
  }

  /**
   * Gets the current value of the asv_bytes_written_last_second counter.
   */
  public static long getCurrentTotalBytesWritten(
      AzureFileSystemInstrumentation instrumentation) {
    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(getMetrics(instrumentation))
      .addCounter(eq(ASV_RAW_BYTES_UPLOADED), anyString(),
        captor.capture());
    return captor.getValue();
  }
}
