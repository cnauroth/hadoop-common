package org.apache.hadoop.fs.azurenative;

import static org.apache.hadoop.test.MetricsAsserts.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.mockito.ArgumentCaptor;

public final class AzureMetricsTestUtil {
  public static final String ASV_WEB_RESPONSES = "asv_web_responses";
  public static final String ASV_BYTES_WRITTEN = "asv_bytes_written_last_second";

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
}
