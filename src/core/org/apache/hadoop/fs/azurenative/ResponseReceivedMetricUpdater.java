package org.apache.hadoop.fs.azurenative;

import java.net.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.azurenative.BandwidthThrottle.ThrottleType;

import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.core.storage.Constants.HeaderConstants;

/**
 * An event listener to the ResponseReceived event from Azure Storage that will
 * update metrics appropriately when it gets that event.
 */
class ResponseReceivedMetricUpdater extends StorageEvent<ResponseReceivedEvent> {

  public static final Log LOG = LogFactory.getLog(ResponseReceivedMetricUpdater.class);

  private final AzureFileSystemInstrumentation instrumentation;
  private final BandwidthGaugeUpdater blockUploadGaugeUpdater;
  private final BandwidthThrottleFeedback bandwidthThrottleFeedback;

  private ResponseReceivedMetricUpdater(OperationContext operationContext,
      AzureFileSystemInstrumentation instrumentation,
      BandwidthGaugeUpdater blockUploadGaugeUpdater,
      BandwidthThrottleFeedback bandwidthThrottleFeedback) {
    this.instrumentation = instrumentation;
    this.blockUploadGaugeUpdater = blockUploadGaugeUpdater;
    this.bandwidthThrottleFeedback = bandwidthThrottleFeedback;
  }

  /**
   * Hooks a new listener to the given operationContext that will update the
   * metrics for the ASV file system appropriately in response to
   * ResponseReceived events.
   *
   * @param operationContext The operationContext to hook.
   * @param instrumentation The metrics source to update.
   * @param blockUploadGaugeUpdater The blockUploadGaugeUpdater to use.
   * @param bandwidthThrottleFeedback Feedbacks tx failures to Azure store.
   * @return
   */
  public static void hook(
      OperationContext operationContext,
      AzureFileSystemInstrumentation instrumentation,
      BandwidthGaugeUpdater blockUploadGaugeUpdater,
      BandwidthThrottleFeedback bandwidthThrottleFeedback) {
    ResponseReceivedMetricUpdater listener =
        new ResponseReceivedMetricUpdater(operationContext,
            instrumentation, blockUploadGaugeUpdater, bandwidthThrottleFeedback);
    operationContext.getResponseReceivedEventHandler().addListener(listener);
  }

  /**
   * Get the content length of the request in the given HTTP connection.
   * @param connection The connection.
   * @return The content length, or zero if not found.
   */
  private long getRequestContentLength(HttpURLConnection connection) {
    String lengthString = connection.getRequestProperty(
        HeaderConstants.CONTENT_LENGTH);
    if (lengthString != null)
      return Long.parseLong(lengthString);
    else
      return 0;
  }

  /**
   * Gets the content length of the response in the given HTTP connection.
   * @param connection The connection.
   * @return The content length.
   */
  private long getResponseContentLength(HttpURLConnection connection) {
    return connection.getContentLength();
  }

  /**
   * Handle the response-received event from Azure SDK.
   */
  @Override
  public void eventOccurred(ResponseReceivedEvent eventArg) {
    instrumentation.webResponse();
    if (!(eventArg.getConnectionObject() instanceof HttpURLConnection)) {
      // Typically this shouldn't happen, but just let it pass
      return;
    }
    HttpURLConnection connection =
        (HttpURLConnection)eventArg.getConnectionObject();
    RequestResult currentResult = eventArg.getRequestResult();
    if (currentResult == null) {
      // Again, typically shouldn't happen, but let it pass
      return;
    }

    long requestLatency = currentResult.getStopDate().getTime() -
                   currentResult.getStartDate().getTime();

    if (currentResult.getStatusCode() == HttpURLConnection.HTTP_CREATED &&
        connection.getRequestMethod().equalsIgnoreCase("PUT")) {
      // If it's a PUT with an HTTP_CREATED status then it's a successful
      // block upload.
      long length = getRequestContentLength(connection);
      if (length > 0) {
        blockUploadGaugeUpdater.blockUploaded(
            currentResult.getStartDate(),
            currentResult.getStopDate(),
            length);
        instrumentation.rawBytesUploaded(length);
        instrumentation.blockUploaded(requestLatency);
      }
      // Simply count all upload transmissions including both meta data, block
      // upload operations, and block list upload operations. This will
      // correspond to the bandwidth metrics calculated above used to calculate
      // throttled bandwidths later on.
      //
      if (null != bandwidthThrottleFeedback) {
        // Send success feedback on upload back to the store.
        //
        bandwidthThrottleFeedback.updateTransmissionSuccess(
            ThrottleType.UPLOAD, 1, requestLatency);
      }
    } else if (currentResult.getStatusCode() == HttpURLConnection.HTTP_PARTIAL &&
        connection.getRequestMethod().equalsIgnoreCase("GET")) {
      // If it's a GET with an HTTP_PARTIAL status then it's a successful
      // block download.
      long length = getResponseContentLength(connection);
      if (length > 0) {
        blockUploadGaugeUpdater.blockDownloaded(
            currentResult.getStartDate(),
            currentResult.getStopDate(),
            length);
        instrumentation.rawBytesDownloaded(length);
        instrumentation.blockDownloaded(requestLatency);
      }

      // Simply count all successful download transmissions.
      //
      if (null != bandwidthThrottleFeedback) {
        // Send success feedback on download back to the store.
        //
        bandwidthThrottleFeedback.updateTransmissionSuccess(
            ThrottleType.DOWNLOAD, 1, requestLatency);
      }
    } else if (currentResult.getStatusCode() ==
                    HttpURLConnection.HTTP_INTERNAL_ERROR ||
               currentResult.getStatusCode() ==
                     HttpURLConnection.HTTP_UNAVAILABLE) {
      // Send failure feedback back to the store.
      //
      if (connection.getRequestMethod().equalsIgnoreCase("PUT")) {
        if (null != bandwidthThrottleFeedback) {
          // Upload failure, update upload failure count.
          //
          bandwidthThrottleFeedback.updateTransmissionFailure(
              ThrottleType.UPLOAD, 1, requestLatency);
        }
      } else if (connection.getRequestMethod().equalsIgnoreCase("GET")){
        if (null != bandwidthThrottleFeedback) {
          // Download failure, update download failure count.
          //
          bandwidthThrottleFeedback.updateTransmissionFailure(
              ThrottleType.DOWNLOAD, 1, requestLatency);
        }
      }
    }
  }
}
