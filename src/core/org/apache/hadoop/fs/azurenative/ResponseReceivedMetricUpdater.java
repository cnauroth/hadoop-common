package org.apache.hadoop.fs.azurenative;

import java.net.*;
import java.util.*;

import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.core.storage.Constants.HeaderConstants;

/**
 * An event listener to the ResponseReceived event from Azure Storage that will
 * update metrics appropriately when it gets that event.
 */
class ResponseReceivedMetricUpdater extends
    StorageEvent<ResponseReceivedEvent> {
  private final AzureFileSystemInstrumentation instrumentation;
  private final OperationContext operationContext;
  private final BlockUploadGaugeUpdater blockUploadGaugeUpdater;

  private ResponseReceivedMetricUpdater(OperationContext operationContext,
      AzureFileSystemInstrumentation instrumentation,
      BlockUploadGaugeUpdater blockUploadGaugeUpdater) {
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
      BlockUploadGaugeUpdater blockUploadGaugeUpdater) {
    ResponseReceivedMetricUpdater listener =
        new ResponseReceivedMetricUpdater(operationContext,
            instrumentation,
            blockUploadGaugeUpdater);
    operationContext.getResponseReceivedEventHandler().addListener(listener);
  }

  private long getRequestContentLength(HttpURLConnection connection) {
    String lengthString = connection.getRequestProperty(
        HeaderConstants.CONTENT_LENGTH);
    if (lengthString != null)
      return Long.parseLong(lengthString);
    else
      return 0;
  }

  private long getResponseContentLength(HttpURLConnection connection) {
    return connection.getContentLength();
  }

  @Override
  public void eventOccurred(ResponseReceivedEvent eventArg) {
    instrumentation.webResponse();
    RequestResult currentResult = operationContext.getLastResult();
    if (!(eventArg.getConnectionObject() instanceof HttpURLConnection)) {
      // Typically this shouldn't happen, but just let it pass
      return;
    }
    HttpURLConnection connection =
        (HttpURLConnection)eventArg.getConnectionObject();
    if (currentResult.getStatusCode() == HttpURLConnection.HTTP_CREATED &&
        connection.getRequestMethod().equalsIgnoreCase("PUT")) {
      // If it's a PUT with an HTTP_CREATED status then it's a successful
      // block upload.
      long length = getRequestContentLength(connection);
      if (length > 0) {
        Date startDate = currentResult.getStartDate();
        Date endDate = currentResult.getStopDate();
        blockUploadGaugeUpdater.blockUploaded(startDate, endDate, length);
        instrumentation.rawBytesUploaded(length);
      }
    } else if (currentResult.getStatusCode() == HttpURLConnection.HTTP_PARTIAL &&
        connection.getRequestMethod().equalsIgnoreCase("GET")) {
      // If it's a GET with an HTTP_PARTIAL status then it's a successful
      // block download.
      long length = getResponseContentLength(connection);
      if (length > 0) {
        instrumentation.rawBytesDownloaded(length);
      }
    }
  }
}
