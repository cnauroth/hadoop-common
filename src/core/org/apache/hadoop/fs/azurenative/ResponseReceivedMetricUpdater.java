package org.apache.hadoop.fs.azurenative;

import java.net.*;

import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.core.storage.Constants.HeaderConstants;
import com.microsoft.windowsazure.services.core.storage.utils.implementation.BaseResponse;

/**
 * An event listener to the ResponseReceived event from Azure Storage that will
 * update metrics appropriately when it gets that event.
 */
class ResponseReceivedMetricUpdater extends
    StorageEvent<ResponseReceivedEvent> {
  private final AzureFileSystemInstrumentation instrumentation;
  private final OperationContext operationContext;
  private final BandwidthGaugeUpdater blockUploadGaugeUpdater;

  private ResponseReceivedMetricUpdater(OperationContext operationContext,
      AzureFileSystemInstrumentation instrumentation,
      BandwidthGaugeUpdater blockUploadGaugeUpdater) {
    this.instrumentation = instrumentation;
    this.operationContext = operationContext;
    this.blockUploadGaugeUpdater = blockUploadGaugeUpdater;
  }

  /**
   * Hooks a new listener to the given operationContext that will update the
   * metrics for the ASV file system appropriately in response to
   * ResponseReceived events. 
   * 
   * @param operationContext The operationContext to hook.
   * @param instrumentation The metrics source to update.
   * @param blockUploadGaugeUpdater The blockUploadGaugeUpdater to use.
   * @return
   */
  public static void hook(
      OperationContext operationContext,
      AzureFileSystemInstrumentation instrumentation,
      BandwidthGaugeUpdater blockUploadGaugeUpdater) {
    ResponseReceivedMetricUpdater listener =
        new ResponseReceivedMetricUpdater(operationContext,
            instrumentation,
            blockUploadGaugeUpdater);
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

  private RequestResult searchForResult(String requestId) {
    if (requestId == null) {
      return null;
    }
    // This code below is not very safe: the requestResults member in
    // operationContext is a non-thread-safe ArrayList, and the SDK can
    // and will add results to it in other threads. I shouldn't be accessing
    // it at all without synchronization, but I don't know how else to get
    // my result. Best I can do for now is to not use an iterator and to get
    // the size upfront. I think this can still break if the ArrayList resizes
    // though - I'll take it up with the SDK people later.
    int numberOfResults = operationContext.getRequestResults().size();
    for (int i = numberOfResults - 1; i >= 0; i--) {
      RequestResult currentRequest = operationContext.getRequestResults().get(i);
      String currentRequestId = currentRequest.getServiceRequestID();
      if (currentRequestId != null && currentRequestId.equals(requestId)) {
        return currentRequest;
      }
    }
    return null;
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
    RequestResult currentResult = searchForResult(
        BaseResponse.getRequestId(connection));
    if (currentResult == null) {
      // Again, typically shouldn't happen, but let it pass
      return;
    }
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
        instrumentation.blockUploaded(
            currentResult.getStopDate().getTime() -
            currentResult.getStartDate().getTime());
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
        instrumentation.blockDownloaded(
            currentResult.getStopDate().getTime() -
            currentResult.getStartDate().getTime());
      }
    }
  }
}
