package org.apache.hadoop.fs.azurenative;

import java.util.*;

/**
 * Internal implementation class to help calculate the current bytes uploaded
 * gauge.
 */
class BlockUploadGaugeUpdater {
  private static final int DEFAULT_WINDOW_SIZE_MS = 1000;
  private int windowSizeMs;
  private ArrayList<BlockWritingWindow> allBlocksWritten =
      new ArrayList<BlockWritingWindow>(1000);
  private final Object blocksWrittenLock = new Object();
  private final AzureFileSystemInstrumentation instrumentation;
  private Thread uploadBandwidthUpdater;

  public BlockUploadGaugeUpdater(AzureFileSystemInstrumentation instrumentation) {
    this(instrumentation, DEFAULT_WINDOW_SIZE_MS, false);
  }

  public BlockUploadGaugeUpdater(AzureFileSystemInstrumentation instrumentation,
      int windowSizeMs, boolean manualUpdateTrigger) {
    this.windowSizeMs = windowSizeMs;
    this.instrumentation = instrumentation;
    if (!manualUpdateTrigger) {
      uploadBandwidthUpdater = new Thread(new UploadBandwidthUpdater());
      uploadBandwidthUpdater.start();
    }
  }

  public void blockUploaded(Date startDate, Date endDate, long length) {
    synchronized (blocksWrittenLock) {
      allBlocksWritten.add(new BlockWritingWindow(startDate, endDate, length));
    }
  }

  /**
   * Triggers the update of the metrics gauge based on all the blocks uploaded
   * so far. This is typically done periodically in a dedicated update thread,
   * but exposing as public for unit test purposes.
   */
  public void triggerUpdate() {
    ArrayList<BlockWritingWindow> toProcess = null;
    synchronized (blocksWrittenLock) {
      if (!allBlocksWritten.isEmpty()) {
        toProcess = allBlocksWritten;
        allBlocksWritten = new ArrayList<BlockWritingWindow>(1000);
      }
    }
    if (toProcess != null) {
      long bytesWrittenInLastSecond = 0;
      long cutoffTime = new Date().getTime() - windowSizeMs;
      for (BlockWritingWindow currentWindow : toProcess) {
        if (currentWindow.getStartDate().getTime() > cutoffTime) {
          bytesWrittenInLastSecond += currentWindow.bytesWritten;
        } else if (currentWindow.getEndDate().getTime() > cutoffTime) {
          long adjustedBytes = (currentWindow.getBytesWritten() *
              (currentWindow.getEndDate().getTime() -
                  cutoffTime)) /
              (currentWindow.getEndDate().getTime() -
                  currentWindow.getStartDate().getTime());
          bytesWrittenInLastSecond += adjustedBytes;
        }
      }
      instrumentation.updateBytesWrittenInLastSecond(bytesWrittenInLastSecond);
    } else {
      instrumentation.updateBytesWrittenInLastSecond(0);
    }
  }

  private static final class BlockWritingWindow {
    private final Date startDate;
    private final Date endDate;
    private final long bytesWritten;

    public BlockWritingWindow(Date startDate, Date endDate, long bytesWritten) {
      this.startDate = startDate;
      this.endDate = endDate;
      this.bytesWritten = bytesWritten;
    }

    public Date getStartDate() { return startDate; }
    public Date getEndDate() { return endDate; }
    public long getBytesWritten() { return bytesWritten; }
  }

  private final class UploadBandwidthUpdater implements Runnable {
    @Override
    public void run() {
      try {
        while (true) {
          Thread.sleep(windowSizeMs);
          triggerUpdate();
        }
      } catch (InterruptedException e) {
      }
    }
  }

  public void close() {
    if (uploadBandwidthUpdater != null) {
      uploadBandwidthUpdater.interrupt();
      try {
        uploadBandwidthUpdater.join();
      } catch (InterruptedException e) {
      }
      uploadBandwidthUpdater = null;
    }
  }
}
