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
import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferSaslUtil.getClientAddress;

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
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.security.SaslInputStream;
import org.apache.hadoop.security.SaslOutputStream;
import org.apache.hadoop.security.SaslRpcServer.QualityOfProtection;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.collect.ImmutableMap;

@InterfaceAudience.Private
public class DataNodeDataTransferProtector {

  private static final Log LOG = LogFactory.getLog(
    DataNodeDataTransferProtector.class);

  private final boolean encryptDataTransfer;
  private final String encryptionAlgorithm;
  private final Map<String, String> saslProps;
  private final TrustedChannelResolver trustedChannelResolver;

  public DataNodeDataTransferProtector(DNConf dnConf) {
    this.encryptDataTransfer = dnConf.getEncryptDataTransfer();
    this.encryptionAlgorithm = dnConf.getEncryptionAlgorithm();
    this.saslProps = ImmutableMap.of(
      Sasl.QOP, dnConf.getDataTransferSaslQop(),
      Sasl.SERVER_AUTH, "true");
    this.trustedChannelResolver = dnConf.getTrustedChannelResolver();
  }

  public IOStreamPair protectStreams(Peer peer, OutputStream underlyingOut,
      InputStream underlyingIn,
      BlockPoolTokenSecretManager blockPoolTokenSecretManager,
      DatanodeID datanodeId) throws IOException {
    if (encryptDataTransfer) {
      return getEncryptedStreams(peer, underlyingOut, underlyingIn,
        blockPoolTokenSecretManager, datanodeId);
    }
    if (UserGroupInformation.isSecurityEnabled()) {
      if (datanodeId.getXferPort() < 1024) {
        return new IOStreamPair(underlyingIn, underlyingOut);
      } else {
        return getSaslStreams(underlyingOut, underlyingIn,
          blockPoolTokenSecretManager, datanodeId);
      }
    }
    return new IOStreamPair(underlyingIn, underlyingOut);
  }

  private IOStreamPair getEncryptedStreams(Peer peer,
      OutputStream underlyingOut, InputStream underlyingIn,
      BlockPoolTokenSecretManager blockPoolTokenSecretManager,
      DatanodeID datanodeId) throws IOException {
    if (!peer.hasSecureChannel() &&
        !trustedChannelResolver.isTrusted(getClientAddress(peer))) {
      return DataTransferEncryptor.getEncryptedStreams(underlyingOut,
        underlyingIn, blockPoolTokenSecretManager, encryptionAlgorithm,
        datanodeId);
    }
    return new IOStreamPair(underlyingIn, underlyingOut);
  }

  private IOStreamPair getSaslStreams(OutputStream underlyingOut,
      InputStream underlyingIn,
      BlockPoolTokenSecretManager blockPoolTokenSecretManager,
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
}
