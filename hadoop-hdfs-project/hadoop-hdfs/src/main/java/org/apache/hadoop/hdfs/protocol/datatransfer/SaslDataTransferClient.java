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

import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferSaslUtil.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;

import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

@InterfaceAudience.Private
public class SaslDataTransferClient {

  private static final Log LOG = LogFactory.getLog(SaslDataTransferClient.class);
  
  /**
   * Delimiter for the three-part SASL username string.
   */
  private static final String NAME_DELIMITER = " ";

  private final SaslPropertiesResolver saslPropsResolver;

  public SaslDataTransferClient(SaslPropertiesResolver saslPropsResolver) {
    this.saslPropsResolver = saslPropsResolver;
  }

  public IOStreamPair saslConnect(Peer peer, DataEncryptionKey encryptionKey,
      Token<BlockTokenIdentifier> accessToken, DatanodeID datanodeId)
      throws IOException {
    return saslConnect(getPeerAddress(peer), peer.getOutputStream(),
      peer.getInputStream(), Suppliers.ofInstance(encryptionKey),
      accessToken, datanodeId);
  }

  public IOStreamPair saslConnect(Socket socket, OutputStream underlyingOut,
      InputStream underlyingIn, DataEncryptionKey encryptionKey,
      Token<BlockTokenIdentifier> accessToken, DatanodeID datanodeId)
      throws IOException {
    Supplier<DataEncryptionKey> encKeySupplier = encryptionKey != null ?
      Suppliers.ofInstance(encryptionKey) : null;
    return saslConnect(socket.getInetAddress(), underlyingOut, underlyingIn,
      encKeySupplier, accessToken, datanodeId);
  }

  public IOStreamPair saslConnect(Socket socket, OutputStream underlyingOut,
      InputStream underlyingIn, Supplier<DataEncryptionKey> encryptionKey,
      Token<BlockTokenIdentifier> accessToken, DatanodeID datanodeId)
      throws IOException {
    return saslConnect(socket.getInetAddress(), underlyingOut, underlyingIn,
      encryptionKey, accessToken, datanodeId);
  }

  private IOStreamPair saslConnect(InetAddress addr, OutputStream underlyingOut,
      InputStream underlyingIn, Supplier<DataEncryptionKey> encryptionKey,
      Token<BlockTokenIdentifier> accessToken, DatanodeID datanodeId)
      throws IOException {
    if (encryptionKey != null) {
      return getEncryptedStreams(underlyingOut, underlyingIn,
        encryptionKey.get(), accessToken, datanodeId);
    }
    if (UserGroupInformation.isSecurityEnabled()) {
      if (datanodeId.getXferPort() < 1024) {
        return new IOStreamPair(underlyingIn, underlyingOut);
      } else {
        return getSaslStreams(addr, underlyingOut, underlyingIn, accessToken,
          datanodeId);
      }
    }
    return new IOStreamPair(underlyingIn, underlyingOut);
  }

  private IOStreamPair getEncryptedStreams(OutputStream underlyingOut,
      InputStream underlyingIn, DataEncryptionKey encryptionKey,
      Token<BlockTokenIdentifier> accessToken, DatanodeID datanodeId)
      throws IOException {
    Map<String, String> saslProps = createSaslPropertiesForEncryption(
      encryptionKey.encryptionAlgorithm);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Client using encryption algorithm " +
          encryptionKey.encryptionAlgorithm);
    }

    String userName = getUserNameFromEncryptionKey(encryptionKey);
    char[] password = encryptionKeyToPassword(encryptionKey.encryptionKey);
    CallbackHandler callbackHandler = new SaslClientCallbackHandler(userName,
      password);
    return doSaslHandshake(underlyingOut, underlyingIn, userName, saslProps,
      callbackHandler);
  }

  /**
   * The SASL username consists of the keyId, blockPoolId, and nonce with the
   * first two encoded as Strings, and the third encoded using Base64. The
   * fields are each separated by a single space.
   * 
   * @param encryptionKey the encryption key to encode as a SASL username.
   * @return encoded username containing keyId, blockPoolId, and nonce
   */
  private static String getUserNameFromEncryptionKey(
      DataEncryptionKey encryptionKey) {
    return encryptionKey.keyId + NAME_DELIMITER +
        encryptionKey.blockPoolId + NAME_DELIMITER +
        new String(Base64.encodeBase64(encryptionKey.nonce, false), Charsets.UTF_8);
  }
  
  /**
   * Set the encryption key when asked by the client-side SASL object.
   */
  private class SaslClientCallbackHandler implements CallbackHandler {

    private final char[] password;
    private final String userName;

    public SaslClientCallbackHandler(String userName, char[] password) {
      this.password = password;
      this.userName = userName;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException,
        UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      RealmCallback rc = null;
      for (Callback callback : callbacks) {
        if (callback instanceof RealmChoiceCallback) {
          continue;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          rc = (RealmCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback,
              "Unrecognized SASL client callback");
        }
      }
      if (nc != null) {
        nc.setName(userName);
      }
      if (pc != null) {
        pc.setPassword(password);
      }
      if (rc != null) {
        rc.setText(rc.getDefaultText());
      }
    }
  }

  private IOStreamPair getSaslStreams(InetAddress addr,
      OutputStream underlyingOut, InputStream underlyingIn,
      Token<BlockTokenIdentifier> accessToken, DatanodeID datanodeId)
      throws IOException {
  /*
      throw new IOException(String.format("Cannot create a secured " +
        "connection if DataNode listens on unprivileged port (%d) and no " +
        "protection is defined in configuration property %s.",
        datanodeId.getXferPort(), DFS_DATA_TRANSFER_PROTECTION_KEY));
  */
    // TODO
    Map<String, String> saslProps = saslPropsResolver.getClientProperties(addr);

    long timestamp = Time.now();
    String userName = buildUserName(accessToken.getIdentifier(), timestamp);
    char[] password = buildClientPassword(accessToken, datanodeId, timestamp);
    CallbackHandler callbackHandler = new SaslClientCallbackHandler(userName,
      password);
    return doSaslHandshake(underlyingOut, underlyingIn, userName, saslProps,
      callbackHandler);
  }

  /**
   * Builds the client's user name, consisting of the base64-encoded serialized
   * block access token identifier and a client-generated timestamp.  Note that
   * this includes only the token identifier, not the token itself, which would
   * include the password.  The password is a shared secret, and we must not
   * write it on the network during the SASL authentication exchange.
   *
   * @param identifier byte[] containing serialized block access token
   *   identifier
   * @param timestamp long client-generated timestamp
   * @return String client's user name
   */
  private static String buildUserName(byte[] identifier, long timestamp) {
    return new String(Base64.encodeBase64(identifier, false), Charsets.UTF_8) +
        NAME_DELIMITER + timestamp;
  }

  /**
   * Calculates the password on the client side.  The password consists of the
   * block access token's password, the target DataNode UUID, and a
   * client-generated request timestamp.
   *
   * @param blockToken Token<BlockTokenIdentifier> for block access
   * @param datanodeId DatanodeID of DataNode receiving connection
   * @return char[] client password
   */    
  private char[] buildClientPassword(Token<BlockTokenIdentifier> blockToken,
      DatanodeID datanodeId, long timestamp) {
    // TOOD: probably want to include block pool ID in password too
    return (new String(Base64.encodeBase64(blockToken.getPassword(), false),
      Charsets.UTF_8) + NAME_DELIMITER + datanodeId.getDatanodeUuid() +
      NAME_DELIMITER + timestamp).toCharArray();
  }

  private IOStreamPair doSaslHandshake(OutputStream underlyingOut,
      InputStream underlyingIn, String userName, Map<String, String> saslProps,
      CallbackHandler callbackHandler) throws IOException {

    DataOutputStream out = new DataOutputStream(underlyingOut);
    DataInputStream in = new DataInputStream(underlyingIn);

    SaslParticipant sasl= SaslParticipant.createClientSaslParticipant(userName,
      saslProps, callbackHandler);

    out.writeInt(SASL_TRANSFER_MAGIC_NUMBER);
    out.flush();

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
      checkSaslComplete(sasl, saslProps);

      return sasl.createStreamPair(out, in);
    } catch (IOException ioe) {
      sendGenericSaslErrorMessage(out, ioe.getMessage());
      throw ioe;
    }
  }
}
