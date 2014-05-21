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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_DEFAULT;
import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferSaslUtil.checkSaslComplete;
import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferSaslUtil.getClientAddress;
import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferSaslUtil.performSaslStep1;
import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferSaslUtil.readSaslMessage;
import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferSaslUtil.sendGenericSaslErrorMessage;
import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferSaslUtil.sendSaslMessage;
import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferSaslUtil.writeMagicNumber;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.TreeMap;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus;
import org.apache.hadoop.hdfs.security.token.block.BlockPoolTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.security.SaslInputStream;
import org.apache.hadoop.security.SaslOutputStream;
import org.apache.hadoop.security.SaslRpcServer.QualityOfProtection;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.collect.ImmutableMap;

@InterfaceAudience.Private
public class SaslDataTransferClient {

  private final boolean encryptDataTransfer;
  private final Map<String, String> saslProps;
  private final TrustedChannelResolver trustedChannelResolver;

  public SaslDataTransferClient(Configuration conf) {
    this.encryptDataTransfer = conf.getBoolean(DFS_ENCRYPT_DATA_TRANSFER_KEY,
      DFS_ENCRYPT_DATA_TRANSFER_DEFAULT);
    String[] qop = conf.getStrings(DFS_DATA_TRANSFER_PROTECTION_KEY,
      DFS_DATA_TRANSFER_PROTECTION_DEFAULT);
    for (int i=0; i < qop.length; i++) {
      qop[i] = QualityOfProtection.valueOf(qop[i].toUpperCase()).getSaslQop();
    }    
    this.saslProps = ImmutableMap.of(
      Sasl.QOP, StringUtils.join(",", qop),
      Sasl.SERVER_AUTH, "true");
    this.trustedChannelResolver = TrustedChannelResolver.getInstance(conf);
  }

  public IOStreamPair protectStreams(Peer peer, OutputStream underlyingOut,
      InputStream underlyingIn, DataEncryptionKey encryptionKey,
      Token<BlockTokenIdentifier> accessToken, DatanodeID datanodeId)
      throws IOException {
    if (encryptDataTransfer) {
      return getEncryptedStreams(peer, underlyingOut, underlyingIn,
        encryptionKey, accessToken, datanodeId);
    }
    if (UserGroupInformation.isSecurityEnabled()) {
      if (datanodeId.getXferPort() < 1024) {
        return new IOStreamPair(underlyingIn, underlyingOut);
      } else {
        return getSaslStreams(underlyingOut, underlyingIn, accessToken,
          datanodeId);
      }
    }
    return new IOStreamPair(underlyingIn, underlyingOut);
  }

  private IOStreamPair getEncryptedStreams(Peer peer,
      OutputStream underlyingOut, InputStream underlyingIn,
      DataEncryptionKey encryptionKey, Token<BlockTokenIdentifier> accessToken,
      DatanodeID datanodeId) throws IOException {
    if (!peer.hasSecureChannel() &&
        !trustedChannelResolver.isTrusted(getClientAddress(peer))) {
      Map<String, String> encryptedSaslProps = ImmutableMap.of(
        Sasl.QOP, "auth-conf",
        Sasl.SERVER_AUTH, "true",
        "com.sun.security.sasl.digest.cipher", encryptionKey.encryptionAlgorithm);
      // TODO
      String userName = null;
      CallbackHandler callbackHandler = null;
      return doSaslHandshake(underlyingOut, underlyingIn, userName,
        encryptedSaslProps, callbackHandler);
    }
    return new IOStreamPair(underlyingIn, underlyingOut);
  }

  private IOStreamPair getSaslStreams(OutputStream underlyingOut,
      InputStream underlyingIn, Token<BlockTokenIdentifier> accessToken,
      DatanodeID datanodeId) throws IOException {
  /*
      throw new IOException(String.format("Cannot create a secured " +
        "connection if DataNode listens on unprivileged port (%d) and no " +
        "protection is defined in configuration property %s.",
        datanodeId.getXferPort(), DFS_DATA_TRANSFER_PROTECTION_KEY));
  */
    // TODO
    return null;
  }

  private IOStreamPair doSaslHandshake(OutputStream underlyingOut,
      InputStream underlyingIn, String userName, Map<String, String> saslProps,
      CallbackHandler callbackHandler) throws IOException {

    DataOutputStream out = new DataOutputStream(underlyingOut);
    DataInputStream in = new DataInputStream(underlyingIn);

    SaslParticipant sasl= SaslParticipant.createClientSaslParticipant(userName,
      saslProps, callbackHandler);

    writeMagicNumber(out);
    
    try {
      // Start of handshake - "initial response" in SASL terminology.
      sendSaslMessage(out, new byte[0]);

      // step 1
      performSaslStep1(out, in, sasl);

      // step 2 (client-side only)
      byte[] remoteResponse = readSaslMessage(in);
      byte[] localResponse = sasl.evaluateChallengeOrResponse(remoteResponse);
      assert localResponse == null;

      // SASL handshake is complete
      checkSaslComplete(sasl);

      return sasl.createEncryptedStreamPair(out, in);
    } catch (IOException ioe) {
      sendGenericSaslErrorMessage(out, ioe.getMessage());
      throw ioe;
    }
  }
}
