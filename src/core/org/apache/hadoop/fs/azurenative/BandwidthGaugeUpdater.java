package org.apache.hadoop.fs.azurenative;

import java.util.*;

/**
 * Internal implementation class to help calculate the current bytes
 * uploaded/downloaded and the maximum bandwidth gauges.
 */
class BandwidthGaugeUpdater {
  private static final int DEFAULT_WINDOW_SIZE_MS = 1000;
  private int windowSizeMs;
  private ArrayList<BlockTransferWindow> allBlocksWritten =
      new ArrayList<BlockTransferWindow>(1000);
  private ArrayList<BlockTransferWindow> allBlocksRead =
      new ArrayList<BlockTransferWindow>(1000);
  private final Object blocksWrittenLock = new Object();
  private final Object blocksReadLock = new Object();
  private final AzureFileSystemInstrumentation instrumentation;
  private Thread uploadBandwidthUpdater;
  private volatile boolean suppressAutoUpdate = false;

  public BandwidthGaugeUpdater(AzureFileSystemInstrumentation instrumentation) {
    this(instrumentation, DEFAULT_WINDOW_SIZE_MS, false);
  }

  public BandwidthGaugeUpdater(AzureFileSystemInstrumentation instrumentation,
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
      allBlocksWritten.add(new BlockTransferWindow(startDate, endDate, length));
    }
  }

  public void blockDownloaded(Date startDate, Date endDate, long length) {
    synchronized (blocksReadLock) {
      allBlocksRead.add(new BlockTransferWindow(startDate, endDate, length));
    }
  }
  
  private void updateBytesTransferred(boolean updateWrite, long bytes) {
    if (updateWrite) {
      instrumentation.updateBytesWrittenInLastSecond(bytes);
    }
    else {
      instrumentation.updateBytesReadInLastSecond(bytes);
    }
  }

  /**
   * For unit test purposes, suppresses auto-update of the metrics
   * from the dedicated thread.
   */
  public void suppressAutoUpdate() {
    suppressAutoUpdate = true;
  }

  /**
   * Resumes auto-update (undo suppressAutoUpdate).
   */
  public void resumeAutoUpdate() {
    suppressAutoUpdate = false;
  }
  
  /**
   * Triggers the update of the metrics gauge based on all the blocks
   * uploaded/downloaded so far. This is typically done periodically in a
   * dedicated update thread, but exposing as public for unit test purposes.
   * 
   * @param updateWrite If true, we'll update the write (upload) metrics.
   *                    Otherwise we'll update the read (download) ones.
   */
  public void triggerUpdate(boolean updateWrite) {
    ArrayList<BlockTransferWindow> toProcess = null;
    synchronized (updateWrite ? blocksWrittenLock : blocksReadLock) {
      if (updateWrite && !allBlocksWritten.isEmpty()) {
        toProcess = allBlocksWritten;
        allBlocksWritten = new ArrayList<BlockTransferWindow>(1000);
      } else if (!updateWrite && !allBlocksRead.isEmpty()) {
        toProcess = allBlocksRead;
        allBlocksRead = new ArrayList<BlockTransferWindow>(1000);        
      }
    }
    if (toProcess != null) {
      long bytesInLastSecond = 0;
      long cutoffTime = new Date().getTime() - windowSizeMs;
      for (BlockTransferWindow currentWindow : toProcess) {
        if (currentWindow.getStartDate().getTime() > cutoffTime) {
          bytesInLastSecond += currentWindow.bytesTransferred;
        } else if (currentWindow.getEndDate().getTime() > cutoffTime) {
          long adjustedBytes = (currentWindow.getBytesTransferred() *
              (currentWindow.getEndDate().getTime() -
                  cutoffTime)) /
              (currentWindow.getEndDate().getTime() -
                  currentWindow.getStartDate().getTime());
          bytesInLastSecond += adjustedBytes;
        }
      }
      updateBytesTransferred(updateWrite, bytesInLastSecond);
    } else {
      updateBytesTransferred(updateWrite, 0);
    }
  }

  private static final class BlockTransferWindow {
    private final Date startDate;
    private final Date endDate;
    private final long bytesTransferred;

    public BlockTransferWindow(Date startDate, Date endDate,
        long bytesTransferred) {
      this.startDate = startDate;
      this.endDate = endDate;
      this.bytesTransferred = bytesTransferred;
    }

    public Date getStartDate() { return startDate; }
    public Date getEndDate() { return endDate; }
    public long getBytesTransferred() { return bytesTransferred; }
  }

  private final class UploadBandwidthUpdater implements Runnable {
    @Override
    public void run() {
      try {
        while (true) {
          Thread.sleep(windowSizeMs);
          if (!suppressAutoUpdate) {
            triggerUpdate(true);
            triggerUpdate(false);
          }
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
