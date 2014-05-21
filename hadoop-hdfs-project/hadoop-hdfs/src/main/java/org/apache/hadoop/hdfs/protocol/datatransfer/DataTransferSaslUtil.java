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
package org.apache.hadoop.hdfs.protocol.datatransfer;

import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus;

import com.google.protobuf.ByteString;

@InterfaceAudience.Private
final class DataTransferSaslUtil {

  /**
   * Sent by clients and validated by servers. We use a number that's unlikely
   * to ever be sent as the value of the DATA_TRANSFER_VERSION.
   */
  public static final int SASL_TRANSFER_MAGIC_NUMBER = 0xDEADBEEF;

  public static void checkMagicNumber(int magicNumber)
      throws InvalidMagicNumberException {
    if (magicNumber != DataTransferSaslUtil.SASL_TRANSFER_MAGIC_NUMBER) {
      throw new InvalidMagicNumberException(magicNumber);
    }
  }

  public static void checkSaslComplete(SaslParticipant sasl) throws IOException {
    if (!sasl.isComplete()) {
      throw new IOException("Failed to complete SASL handshake");
    }
  }

  public static void performSaslStep1(DataOutputStream out, DataInputStream in,
      SaslParticipant sasl) throws IOException {
    byte[] remoteResponse = readSaslMessage(in);
    byte[] localResponse = sasl.evaluateChallengeOrResponse(remoteResponse);
    sendSaslMessage(out, localResponse);
  }
  
  public static byte[] readSaslMessage(DataInputStream in) throws IOException {
    DataTransferEncryptorMessageProto proto =
        DataTransferEncryptorMessageProto.parseFrom(vintPrefixed(in));
    if (proto.getStatus() == DataTransferEncryptorStatus.ERROR_UNKNOWN_KEY) {
      throw new InvalidEncryptionKeyException(proto.getMessage());
    } else if (proto.getStatus() == DataTransferEncryptorStatus.ERROR) {
      throw new IOException(proto.getMessage());
    } else {
      return proto.getPayload().toByteArray();
    }
  }

  public static void sendGenericSaslErrorMessage(DataOutputStream out,
      String message) throws IOException {
    sendSaslMessage(out, DataTransferEncryptorStatus.ERROR, null, message);
  }

  public static void sendSaslMessage(DataOutputStream out, byte[] payload)
      throws IOException {
    sendSaslMessage(out, DataTransferEncryptorStatus.SUCCESS, payload, null);
  }
  
  public static void sendSaslMessage(OutputStream out,
      DataTransferEncryptorStatus status, byte[] payload, String message)
          throws IOException {
    DataTransferEncryptorMessageProto.Builder builder =
        DataTransferEncryptorMessageProto.newBuilder();
    
    builder.setStatus(status);
    if (payload != null) {
      builder.setPayload(ByteString.copyFrom(payload));
    }
    if (message != null) {
      builder.setMessage(message);
    }
    
    DataTransferEncryptorMessageProto proto = builder.build();
    proto.writeDelimitedTo(out);
    out.flush();
  }

  private DataTransferSaslUtil() {
  }
}
