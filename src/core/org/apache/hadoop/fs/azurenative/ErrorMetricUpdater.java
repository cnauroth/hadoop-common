package org.apache.hadoop.fs.azurenative;

import static java.net.HttpURLConnection.*;

import com.microsoft.windowsazure.services.core.storage.*;

/**
 * An event listener to the ResponseReceived event from Azure Storage that will
 * update error metrics appropriately when it gets that event.
 */
public class ErrorMetricUpdater extends StorageEvent<ResponseReceivedEvent> {
  private final AzureFileSystemInstrumentation instrumentation;
  private final OperationContext operationContext;

  private ErrorMetricUpdater(OperationContext operationContext,
      AzureFileSystemInstrumentation instrumentation) {
    this.instrumentation = instrumentation;
    this.operationContext = operationContext;
  }

  /**
   * Hooks a new listener to the given operationContext that will update the
   * error metrics for the ASV file system appropriately in response to
   * ResponseReceived events.
   *
   * @param operationContext The operationContext to hook.
   * @param instrumentation The metrics source to update.
   * @return
   */
  public static void hook(
      OperationContext operationContext,
      AzureFileSystemInstrumentation instrumentation) {
    ErrorMetricUpdater listener =
        new ErrorMetricUpdater(operationContext,
            instrumentation);
    operationContext.getResponseReceivedEventHandler().addListener(listener);
  }

  @Override
  public void eventOccurred(ResponseReceivedEvent eventArg) {
    RequestResult currentResult = operationContext.getLastResult();
    switch (currentResult.getStatusCode()) {
    case HTTP_OK:
    case HTTP_ACCEPTED:
    case HTTP_CREATED:
    case HTTP_PARTIAL:
      // Success!
      break;
    case HTTP_NOT_FOUND:
      // Not success, but very common (e.g. every check for exists()
      // generates it). Just ignore.
      break;
    case HTTP_INTERNAL_ERROR:
    case HTTP_UNAVAILABLE:
      // Either Azure Storage bug or (more likely) throttling.
      instrumentation.serverErrorEncountered();
      break;
    default:
      // Assume it's one of the many possible errors caused by user actions
      // e.g. 403 for authentication failed, 409/412 for lease violations, ...
      instrumentation.userErrorEncountered();
      break;
    }
  }
}
