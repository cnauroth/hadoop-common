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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.SaslException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus;
import org.apache.hadoop.hdfs.security.token.block.BlockPoolTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import com.google.common.base.Charsets;
import com.google.common.base.Supplier;

@InterfaceAudience.Private
public class SaslDataTransferServer {

  private static final Log LOG = LogFactory.getLog(SaslDataTransferServer.class);
  
  /**
   * Delimiter for the three-part SASL username string.
   */
  private static final String NAME_DELIMITER = " ";

  private final BlockPoolTokenSecretManager blockPoolTokenSecretManager;
  private final DNConf dnConf;
  private final SaslDataTransferClient saslDataTransferClient;

  public SaslDataTransferServer(DNConf dnConf,
      BlockPoolTokenSecretManager blockPoolTokenSecretManager) {
    this.blockPoolTokenSecretManager = blockPoolTokenSecretManager;
    this.dnConf = dnConf;
    this.saslDataTransferClient = new SaslDataTransferClient(
      dnConf.getSaslPropsResolver());
  }

  public IOStreamPair saslClientConnect(Socket socket,
      OutputStream underlyingOut, InputStream underlyingIn,
      final ExtendedBlock block, Token<BlockTokenIdentifier> accessToken,
      DatanodeID datanodeId) throws IOException {
    Supplier<DataEncryptionKey> encryptionKeySupplier =
      new Supplier<DataEncryptionKey>() {
        @Override
        public DataEncryptionKey get() {
          return blockPoolTokenSecretManager.generateDataEncryptionKey(
            block.getBlockPoolId());
        }
      };
    return saslDataTransferClient.saslConnect(socket, underlyingOut,
      underlyingIn, encryptionKeySupplier, accessToken, datanodeId);
  }

  public IOStreamPair saslConnect(Peer peer, OutputStream underlyingOut,
      InputStream underlyingIn, DatanodeID datanodeId) throws IOException {
    if (dnConf.getEncryptDataTransfer()) {
      return getEncryptedStreams(peer, underlyingOut, underlyingIn, datanodeId);
    }
    if (UserGroupInformation.isSecurityEnabled()) {
      if (datanodeId.getXferPort() < 1024) {
        return new IOStreamPair(underlyingIn, underlyingOut);
      } else {
        return getSaslStreams(peer, underlyingOut, underlyingIn, datanodeId);
      }
    }
    return new IOStreamPair(underlyingIn, underlyingOut);
  }

  private IOStreamPair getEncryptedStreams(Peer peer,
      OutputStream underlyingOut, InputStream underlyingIn,
      DatanodeID datanodeId) throws IOException {
    if (!peer.hasSecureChannel() &&
        !dnConf.getTrustedChannelResolver().isTrusted(getPeerAddress(peer))) {
      Map<String, String> saslProps = createSaslPropertiesForEncryption(
        dnConf.getEncryptionAlgorithm());

      if (LOG.isDebugEnabled()) {
        LOG.debug("Server using encryption algorithm " +
          dnConf.getEncryptionAlgorithm());
      }

      CallbackHandler callbackHandler = new SaslServerCallbackHandler(
        new PasswordFunction() {
          @Override
          public char[] apply(String userName) throws IOException {
            return encryptionKeyToPassword(getEncryptionKeyFromUserName(
              userName));
          }
      });
      SaslParticipant sasl = SaslParticipant.createServerSaslParticipant(
        saslProps, callbackHandler);
      return doSaslHandshake(underlyingOut, underlyingIn, saslProps,
        callbackHandler);
    }
    return new IOStreamPair(underlyingIn, underlyingOut);
  }

  private interface PasswordFunction {
    char[] apply(String userName) throws IOException;
  }

  /**
   * Set the encryption key when asked by the server-side SASL object.
   */
  private class SaslServerCallbackHandler implements CallbackHandler {

    private final PasswordFunction passwordFunction;

    public SaslServerCallbackHandler(PasswordFunction passwordFunction) {
      this.passwordFunction = passwordFunction;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException,
        UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof RealmCallback) {
          continue; // realm is ignored
        } else {
          throw new UnsupportedCallbackException(callback,
              "Unrecognized SASL DIGEST-MD5 Callback: " + callback);
        }
      }

      if (pc != null) {
        byte[] encryptionKey = getEncryptionKeyFromUserName(nc.getDefaultName());
        pc.setPassword(passwordFunction.apply(nc.getDefaultName()));
      }

      if (ac != null) {
        ac.setAuthorized(true);
        ac.setAuthorizedID(ac.getAuthorizationID());
      }
    }
  }

  /**
   * Given a secret manager and a username encoded as described above, determine
   * the encryption key.
   * 
   * @param userName containing the keyId, blockPoolId, and nonce.
   * @return secret encryption key.
   * @throws IOException
   */
  private byte[] getEncryptionKeyFromUserName(String userName)
      throws IOException {
    String[] nameComponents = userName.split(NAME_DELIMITER);
    if (nameComponents.length != 3) {
      throw new IOException("Provided name '" + userName + "' has " +
          nameComponents.length + " components instead of the expected 3.");
    }
    int keyId = Integer.parseInt(nameComponents[0]);
    String blockPoolId = nameComponents[1];
    byte[] nonce = Base64.decodeBase64(nameComponents[2]);
    return blockPoolTokenSecretManager.retrieveDataEncryptionKey(keyId,
        blockPoolId, nonce);
  }

  private IOStreamPair getSaslStreams(Peer peer, OutputStream underlyingOut,
      InputStream underlyingIn, final DatanodeID datanodeId) throws IOException {
  /*
      throw new IOException(String.format("Cannot create a secured " +
        "connection if DataNode listens on unprivileged port (%d) and no " +
        "protection is defined in configuration property %s.",
        datanodeId.getXferPort(), DFS_DATA_TRANSFER_PROTECTION_KEY));
  */
    // TODO
    Map<String, String> saslProps = dnConf.getSaslPropsResolver()
      .getServerProperties(getPeerAddress(peer));

    CallbackHandler callbackHandler = new SaslServerCallbackHandler(
      new PasswordFunction() {
        @Override
        public char[] apply(String userName) throws IOException {
          return buildServerPassword(userName, datanodeId);
        }
    });
    SaslParticipant sasl = SaslParticipant.createServerSaslParticipant(
      saslProps, callbackHandler);
    return doSaslHandshake(underlyingOut, underlyingIn, saslProps,
        callbackHandler);
  }

  /**
   * Calculates the expected correct password on the server side.  The
   * password consists of the block access token's password (known to the
   * DataNode via its secret manager), the target DataNode UUID (also known to
   * the DataNode), and a request timestamp (provided in the client request).
   * This expects that the client has supplied a user name consisting of its
   * serialized block access token identifier and the client-generated
   * timestamp.  The timestamp is checked against a configurable expiration to
   * make replay attacks harder.
   *
   * @param userName String user name containing serialized block access token
   *   and client-generated timestamp
   * @param datanodeId DatanodeID of DataNode accepting connection
   * @return char[] expected correct password
   * @throws IOException if there is any I/O error
   */    
  private char[] buildServerPassword(String userName, DatanodeID datanodeId)
      throws IOException {
    // TOOD: probably want to include block pool ID in password too
    String[] parts = userName.split(NAME_DELIMITER);
    String[] nameComponents = userName.split(NAME_DELIMITER);
    if (nameComponents.length != 2) {
      throw new IOException("Provided name '" + userName + "' has " +
        nameComponents.length + " components instead of the expected 2.");
    }
    BlockTokenIdentifier identifier = deserializeIdentifier(nameComponents[0]);
    long timestamp = Long.parseLong(nameComponents[1]);
    // TODO: Check timestamp within configurable threshold.
    byte[] tokenPassword = blockPoolTokenSecretManager.retrievePassword(
      identifier);
    return (new String(Base64.encodeBase64(tokenPassword, false),
      Charsets.UTF_8) + NAME_DELIMITER + datanodeId.getDatanodeUuid() +
      NAME_DELIMITER + timestamp).toCharArray();
  }

  /**
   * Deserializes a base64-encoded binary representation of a block access
   * token.
   *
   * @param str String to deserialize
   * @return BlockTokenIdentifier deserialized from str
   * @throws IOException if there is any I/O error
   */
  private BlockTokenIdentifier deserializeIdentifier(String str)
      throws IOException {
    BlockTokenIdentifier identifier = new BlockTokenIdentifier();
    identifier.readFields(new DataInputStream(new ByteArrayInputStream(
      Base64.decodeBase64(str))));
    return identifier;
  }

  private IOStreamPair doSaslHandshake(OutputStream underlyingOut,
      InputStream underlyingIn, Map<String, String> saslProps,
      CallbackHandler callbackHandler) throws IOException {

    DataInputStream in = new DataInputStream(underlyingIn);
    DataOutputStream out = new DataOutputStream(underlyingOut);

    SaslParticipant sasl = SaslParticipant.createServerSaslParticipant(saslProps,
      callbackHandler);

    int magicNumber = in.readInt();
    if (magicNumber != SASL_TRANSFER_MAGIC_NUMBER) {
      throw new InvalidMagicNumberException(magicNumber);
    }
    try {
      // step 1
      performSaslStep1(out, in, sasl);

      // step 2 (server-side only)
      byte[] remoteResponse = readSaslMessage(in);
      byte[] localResponse = sasl.evaluateChallengeOrResponse(remoteResponse);
      sendSaslMessage(out, localResponse);

      // SASL handshake is complete
      checkSaslComplete(sasl, saslProps);

      return sasl.createStreamPair(out, in);
    } catch (IOException ioe) {
      if (ioe instanceof SaslException &&
          ioe.getCause() != null &&
          ioe.getCause() instanceof InvalidEncryptionKeyException) {
        // This could just be because the client is long-lived and hasn't gotten
        // a new encryption key from the NN in a while. Upon receiving this
        // error, the client will get a new encryption key from the NN and retry
        // connecting to this DN.
        sendInvalidKeySaslErrorMessage(out, ioe.getCause().getMessage());
      } else {
        sendGenericSaslErrorMessage(out, ioe.getMessage());
      }
      throw ioe;
    }
  }

  private static void sendInvalidKeySaslErrorMessage(DataOutputStream out,
      String message) throws IOException {
    sendSaslMessage(out, DataTransferEncryptorStatus.ERROR_UNKNOWN_KEY, null,
        message);
  }
}
