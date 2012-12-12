package org.apache.hadoop.fs.azurenative;

import static org.apache.hadoop.fs.azurenative.AzureMetricsTestUtil.*;

import java.util.*;

import junit.framework.*;

public class TestBlockUploadGaugeUpdater extends TestCase {
  public void testSingleThreaded() throws Exception {
    AzureFileSystemInstrumentation instrumentation =
        new AzureFileSystemInstrumentation();
    BlockUploadGaugeUpdater updater =
        new BlockUploadGaugeUpdater(instrumentation, 1000, true);
    updater.triggerUpdate();
    assertEquals(0, getCurrentBytesWritten(instrumentation));
    updater.blockUploaded(new Date(), new Date(), 150);
    updater.triggerUpdate();
    assertEquals(150, getCurrentBytesWritten(instrumentation));
    updater.blockUploaded(new Date(new Date().getTime() - 10000),
        new Date(), 200);
    updater.triggerUpdate();
    long currentBytes = getCurrentBytesWritten(instrumentation);
    assertTrue(
        "We expect around (200/10 = 20) bytes written as the gauge value." +
        "Got " + currentBytes,
        currentBytes > 18 && currentBytes < 22);
    updater.close();
  }

  public void testMultiThreaded() throws Exception {
    final AzureFileSystemInstrumentation instrumentation =
        new AzureFileSystemInstrumentation();
    final BlockUploadGaugeUpdater updater =
        new BlockUploadGaugeUpdater(instrumentation, 1000, true);
    Thread[] threads = new Thread[10];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          updater.blockUploaded(new Date(), new Date(), 10);
          updater.blockUploaded(new Date(0), new Date(0), 10);
        }
      });
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    updater.triggerUpdate();
    assertEquals(10 * threads.length, getCurrentBytesWritten(instrumentation));
    updater.close();
  }
}
