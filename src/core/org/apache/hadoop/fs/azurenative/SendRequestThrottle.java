/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurenative;

import java.net.HttpURLConnection;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.azurenative.AzureNativeFileSystemStore.ThrottleType;

import com.microsoft.windowsazure.services.core.storage.Constants.HeaderConstants;
import com.microsoft.windowsazure.services.core.storage.OperationContext;
import com.microsoft.windowsazure.services.core.storage.SendingRequestEvent;
import com.microsoft.windowsazure.services.core.storage.StorageEvent;


/**
 * Manages the lifetime of binding on the operation contexts to intercept
 * send request events to Azure storage. Sends are throttled by calling back
 * on the ThrottleSendRequestCallback which determines how long the thread
 * should delay the send.
 *
 */
public class SendRequestThrottle  extends StorageEvent<SendingRequestEvent>  {

  public static final Log LOG = LogFactory.getLog(SendRequestThrottle.class);
  private static final long DEFAULT_BLOCK_UPLOAD_SIZE = 4 * 1024 * 1024;
  private static final String ALLOW_ALL_REQUEST_PRECONDITIONS = "*";
  private final ThrottleSendRequestCallback sendRequestCallback;
  private final boolean allowConcurrentReadWrite; // Check for blob changes.

  /**
   * Constructor for SendRequestThrottle.
   * @param sendDelayCallback
   */
  private SendRequestThrottle(OperationContext opContext,
      ThrottleSendRequestCallback sendRequestCallback) {
    // Capture the send delay callback interface.
    //
    this (opContext, sendRequestCallback, false);
  }

  /**
   * Constructor for SendRequestThrottle.
   * @param sendDelayCallback
   */
  private SendRequestThrottle(OperationContext opContext,
      ThrottleSendRequestCallback sendRequestCallback, boolean allowConcurrentReadWrite) {
    // Capture the send delay callback interface.
    //
    this.sendRequestCallback = sendRequestCallback;
    this.allowConcurrentReadWrite = allowConcurrentReadWrite;
  }

  /**
   * Binds a new lister to the operation context so  the ASV file system
   * can appropriately delay sends and throttle bandwidth in response to
   * SendingRequest events. Check for the immutability of the blob to ensure
   * there are no concurrent reads and writes on the blob.
   *
   * @param opContext The operation context to bind to listener.
   * @param sendRequestCalback for delays on current send thread.
   *
   */
  public static void bind(OperationContext opContext,
      ThrottleSendRequestCallback sendRequestCallback) {
    bind (opContext, sendRequestCallback, false);
  }

  /**
   * Binds a new lister to the operation context so  the ASV file system
   * can appropriately delay sends and throttle bandwidth in response to
   * SendingRequest events. This function allows bypassing the blob immutability
   * check when reading streams. By bypassing the immutability check, concurrent
   * reads and writes are allowed on the blob.
   *
   * @param opContext The operation context to bind to listener.
   * @param sendRequestCalback for delays on current send thread.
   * @param allowConcurrentReadWrite do not check for immutability of the blob.
   *
   */
  public static void bind(OperationContext opContext,
      ThrottleSendRequestCallback sendRequestCallback, boolean allowConcurrentReadWrite) {
    SendRequestThrottle sendListener =
        new SendRequestThrottle(opContext, sendRequestCallback, allowConcurrentReadWrite);

    opContext.getSendingRequestEventHandler().addListener(sendListener);
  }

  /**
   * Handler which processes the sending request event from Azure SDK.
   * The handler simply pauses the thread for the duration returned by
   * the duration of the delay returned by the send delay callback.
   *
   * @param sendEvent - send event context from Windows Azure SDK.
   */
  @Override
  public void eventOccurred(SendingRequestEvent sendEvent) {

    if (!(sendEvent.getConnectionObject() instanceof HttpURLConnection)) {
      // Pass if there is no HTTP connection associated with this send
      // request.
      return;
    }

    // Capture the HTTP URL connection object and get size of the payload for the
    // request.
    //
    HttpURLConnection urlConnection =
        (HttpURLConnection) sendEvent.getConnectionObject();

    // Reset pay load size.
    //
    long payloadSize = 0;

    // Determine whether this is an upload or download request by examining the
    // request method. A request method of "PUT" indicates an upload and a request
    // method of "GET" indicates a download.
    //
    if (urlConnection.getRequestMethod().equalsIgnoreCase("PUT")) {
      // This is a Put Block request. Get the content length.
      //
      String payloadLengthProperty =
          urlConnection.getRequestProperty(HeaderConstants.CONTENT_LENGTH);

      // Note: From documentation the content length should is required for PUT
      //       block requests and the block must be <= 4MB.  However, no
      //       content length is associated with block upload requests. Verify
      //       this with the Azure storage team.  If no content length assume
      //       a 4 MB upload.
      //
      payloadSize = DEFAULT_BLOCK_UPLOAD_SIZE;
      if (null != payloadLengthProperty) {
        payloadSize = Long.parseLong(payloadLengthProperty);
      }

      // Callback to throttle send request.
      //
      if (payloadSize > 0) {
        sendRequestCallback.throttleSendRequest(ThrottleType.UPLOAD, payloadSize);

        // Reset the start time of the request ensure the latency of the request
        // is not skewed by the latency of the throttleSendRequest callback.
        //
        // Note: Use the java.utility.Date class here because that is used by the
        //       Windows Azure SDK for setting the start date.
        sendEvent.getRequestResult().setStartDate(new Date());
      }
    } else if (urlConnection.getRequestMethod().equalsIgnoreCase("GET")) {
      // This is a DOWNLOAD request. Check that the x-ms-range property is set.
      //
      String payloadRangeProperty =
          urlConnection.getRequestProperty(HeaderConstants.STORAGE_RANGE_HEADER);
      if (null != payloadRangeProperty) {
        String[] byteRange = payloadRangeProperty.split("[^\\d]+");
        long offsetLo = Long.parseLong(byteRange[1]);
        long offsetHi = Long.parseLong(byteRange[2]);

        // Calculate the payload from the offset range.
        //
        payloadSize = offsetHi - offsetLo  + 1;
      }

      // If concurrent reads/write are allowed, reset the if-match condition on the
      // conditional header.
      //
      if (allowConcurrentReadWrite) {
        // Set the if-match conditional header to allow all preconditions.
        //
        urlConnection.setRequestProperty(HeaderConstants.IF_MATCH, ALLOW_ALL_REQUEST_PRECONDITIONS);
      }

      if (payloadSize > 0) {
        sendRequestCallback.throttleSendRequest(ThrottleType.DOWNLOAD, payloadSize);

        // Reset the start time of the request ensure the latency of the request
        // is not skewed by the latency of the throttleSendRequest callback.
        //
        // Note: Use the java.utility.Date class here because that is used by the
        //       Windows Azure SDK for setting the start date.
        //
        sendEvent.getRequestResult().setStartDate(new Date());
      }
    }
  }
}
