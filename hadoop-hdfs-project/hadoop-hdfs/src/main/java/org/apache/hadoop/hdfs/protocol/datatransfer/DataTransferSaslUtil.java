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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_TRANSFER_SASL_PROPS_RESOLVER_CLASS_KEY;
import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.Map;
import javax.security.sasl.Sasl;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SaslRpcServer.QualityOfProtection;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import com.google.protobuf.ByteString;

@InterfaceAudience.Private
final class DataTransferSaslUtil {

  /**
   * Delimiter for the three-part SASL username string.
   */
  public static final String NAME_DELIMITER = " ";

  /**
   * Sent by clients and validated by servers. We use a number that's unlikely
   * to ever be sent as the value of the DATA_TRANSFER_VERSION.
   */
  public static final int SASL_TRANSFER_MAGIC_NUMBER = 0xDEADBEEF;

  public static void checkSaslComplete(SaslParticipant sasl) throws IOException {
    if (!sasl.isComplete()) {
      throw new IOException("Failed to complete SASL handshake");
    }

    if (!sasl.supportsConfidentiality()) {
      throw new IOException("SASL handshake completed, but channel does not " +
          "support encryption");
    }
  }

  public static void checkSaslSupportsConfidentiality(SaslParticipant sasl)
      throws IOException {
  }

  public static Map<String, String> createSaslPropertiesForEncryption(
      String encryptionAlgorithm) {
    return ImmutableMap.of(
      Sasl.QOP, QualityOfProtection.PRIVACY.getSaslQop(),
      Sasl.SERVER_AUTH, "true",
      "com.sun.security.sasl.digest.cipher", encryptionAlgorithm);
  }

  public static char[] encryptionKeyToPassword(byte[] encryptionKey) {
    return new String(Base64.encodeBase64(encryptionKey, false), Charsets.UTF_8)
      .toCharArray();
  }

  /**
   * Returns InetAddress from peer
   * The getRemoteAddressString is the form  /ip-address:port
   * The ip-address is extracted from peer and InetAddress is formed
   * @param peer
   * @return
   * @throws UnknownHostException
   */
  public static InetAddress getClientAddress(Peer peer) {
    return InetAddresses.forString(
        peer.getRemoteAddressString().split(":")[0].substring(1));
  }

  public static SaslPropertiesResolver getSaslPropertiesResolver(
      Configuration conf) {
    Configuration saslPropsResolverConf = new Configuration(conf);
    saslPropsResolverConf.set(HADOOP_RPC_PROTECTION,
      conf.get(DFS_DATA_TRANSFER_PROTECTION_KEY,
        DFS_DATA_TRANSFER_PROTECTION_DEFAULT));
    saslPropsResolverConf.setClass(HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS,
      conf.getClass(DFS_DATA_TRANSFER_SASL_PROPS_RESOLVER_CLASS_KEY,
        SaslPropertiesResolver.class), SaslPropertiesResolver.class);
    return SaslPropertiesResolver.getInstance(conf);
  }

  public static void performSaslStep1(OutputStream out, InputStream in,
      SaslParticipant sasl) throws IOException {
    byte[] remoteResponse = readSaslMessage(in);
    byte[] localResponse = sasl.evaluateChallengeOrResponse(remoteResponse);
    sendSaslMessage(out, localResponse);
  }
  
  public static byte[] readSaslMessage(InputStream in) throws IOException {
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

  public static void sendGenericSaslErrorMessage(OutputStream out,
      String message) throws IOException {
    sendSaslMessage(out, DataTransferEncryptorStatus.ERROR, null, message);
  }

  public static void sendSaslMessage(OutputStream out, byte[] payload)
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
