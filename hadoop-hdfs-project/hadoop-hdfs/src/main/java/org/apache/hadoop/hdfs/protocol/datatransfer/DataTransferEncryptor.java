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

import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferSaslUtil.checkSaslComplete;
import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferSaslUtil.performSaslStep1;
import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferSaslUtil.readMagicNumber;
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
import javax.security.sasl.SaslException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus;
import org.apache.hadoop.hdfs.security.token.block.BlockPoolTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.security.SaslInputStream;
import org.apache.hadoop.security.SaslOutputStream;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;

/**
 * A class which, given connected input/output streams, will perform a
 * handshake using those streams based on SASL to produce new Input/Output
 * streams which will encrypt/decrypt all data written/read from said streams.
 * Much of this is inspired by or borrowed from the TSaslTransport in Apache
 * Thrift, but with some HDFS-specific tweaks.
 */
@InterfaceAudience.Private
public class DataTransferEncryptor {
  
  public static final Log LOG = LogFactory.getLog(DataTransferEncryptor.class);
  
  /**
   * Delimiter for the three-part SASL username string.
   */
  private static final String NAME_DELIMITER = " ";
  private static final Map<String, String> SASL_PROPS = new TreeMap<String, String>();
  
  static {
    SASL_PROPS.put(Sasl.QOP, "auth");
    SASL_PROPS.put(Sasl.SERVER_AUTH, "true");
  }
  
  /**
   * Factory method for DNs, where the nonce, keyId, and encryption key are not
   * yet known. The nonce and keyId will be sent by the client, and the DN
   * will then use those pieces of info and the secret key shared with the NN
   * to determine the encryptionKey used for the SASL handshake/encryption.
   * 
   * Establishes a secure connection assuming that the party on the other end
   * has the same shared secret. This does a SASL connection handshake, but not
   * a general-purpose one. It's specific to the MD5-DIGEST SASL mechanism with
   * auth-conf enabled. In particular, it doesn't support an arbitrary number of
   * challenge/response rounds, and we know that the client will never have an
   * initial response, so we don't check for one.
   *
   * @param underlyingOut output stream to write to the other party
   * @param underlyingIn input stream to read from the other party
   * @param blockPoolTokenSecretManager secret manager capable of constructing
   *        encryption key based on keyId, blockPoolId, and nonce
   * @return a pair of streams which wrap the given streams and encrypt/decrypt
   *         all data read/written
   * @throws IOException in the event of error
   */
  public static IOStreamPair getEncryptedStreams(
      OutputStream underlyingOut, InputStream underlyingIn,
      BlockPoolTokenSecretManager blockPoolTokenSecretManager,
      String encryptionAlgorithm, DatanodeID datanodeId) throws IOException {
    
    DataInputStream in = new DataInputStream(underlyingIn);
    DataOutputStream out = new DataOutputStream(underlyingOut);
    
    Map<String, String> saslProps = Maps.newHashMap(SASL_PROPS);
    saslProps.put("com.sun.security.sasl.digest.cipher", encryptionAlgorithm);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Server using encryption algorithm " + encryptionAlgorithm);
    }
    
    SaslParticipant sasl = SaslParticipant.createServerSaslParticipant(saslProps,
      new SaslServerCallbackHandler(blockPoolTokenSecretManager, datanodeId));
    
    readMagicNumber(in);
    try {
      // step 1
      performSaslStep1(out, in, sasl);
      
      // step 2 (server-side only)
      byte[] remoteResponse = readSaslMessage(in);
      byte[] localResponse = sasl.evaluateChallengeOrResponse(remoteResponse);
      sendSaslMessage(out, localResponse);
      
      // SASL handshake is complete
      checkSaslComplete(sasl);
      
      return sasl.createEncryptedStreamPair(out, in);
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
  
  /**
   * Factory method for clients, where the encryption token is already created.
   * 
   * Establishes a secure connection assuming that the party on the other end
   * has the same shared secret. This does a SASL connection handshake, but not
   * a general-purpose one. It's specific to the MD5-DIGEST SASL mechanism with
   * auth-conf enabled. In particular, it doesn't support an arbitrary number of
   * challenge/response rounds, and we know that the client will never have an
   * initial response, so we don't check for one.
   *
   * @param underlyingOut output stream to write to the other party
   * @param underlyingIn input stream to read from the other party
   * @param encryptionKey all info required to establish an encrypted stream
   * @return a pair of streams which wrap the given streams and encrypt/decrypt
   *         all data read/written
   * @throws IOException in the event of error
   */
  public static IOStreamPair getEncryptedStreams(
      OutputStream underlyingOut, InputStream underlyingIn,
      DataEncryptionKey encryptionKey, Token<BlockTokenIdentifier> blockToken,
      DatanodeID datanodeId)
          throws IOException {
    
    Map<String, String> saslProps = Maps.newHashMap(SASL_PROPS);
    saslProps.put("com.sun.security.sasl.digest.cipher",
        encryptionKey.encryptionAlgorithm);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client using encryption algorithm " +
          encryptionKey.encryptionAlgorithm);
    }
    
    DataOutputStream out = new DataOutputStream(underlyingOut);
    DataInputStream in = new DataInputStream(underlyingIn);
    
    long timestamp = Time.now();
    String userName = buildUserName(blockToken.getIdentifier(), timestamp);
    SaslParticipant sasl= SaslParticipant.createClientSaslParticipant(userName,
      saslProps, new SaslClientCallbackHandler(encryptionKey.encryptionKey,
        blockToken, datanodeId, userName, timestamp));
    
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
  
  private static void sendInvalidKeySaslErrorMessage(DataOutputStream out,
      String message) throws IOException {
    sendSaslMessage(out, DataTransferEncryptorStatus.ERROR_UNKNOWN_KEY, null,
        message);
  }
  
  /**
   * Set the encryption key when asked by the server-side SASL object.
   */
  private static class SaslServerCallbackHandler implements CallbackHandler {
    
    private final BlockPoolTokenSecretManager blockPoolTokenSecretManager;
    private final DatanodeID datanodeId;
    
    public SaslServerCallbackHandler(BlockPoolTokenSecretManager
        blockPoolTokenSecretManager, DatanodeID datanodeId) {
      this.blockPoolTokenSecretManager = blockPoolTokenSecretManager;
      this.datanodeId = datanodeId;
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
        pc.setPassword(buildServerPassword(nc.getDefaultName()));
      }
      
      if (ac != null) {
        ac.setAuthorized(true);
        ac.setAuthorizedID(ac.getAuthorizationID());
      }
      
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
     * @return char[] expected correct password
     * @throws IOException if there is any I/O error
     */    
    private char[] buildServerPassword(String userName) throws IOException {
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
     * Deserializes a base64-encoded binary representatino of a block access
     * token.
     *
     * @param str String to deserialize
     * @return BlockTokenIdentifier deserialized from str
     * @throws IOException if there is any I/O error
     */
    private BlockTokenIdentifier deserializeIdentifier(String str)
        throws IOException {
      BlockTokenIdentifier identifier = new BlockTokenIdentifier();
      identifier.readFields(new DataInputStream(
        new ByteArrayInputStream(Base64.decodeBase64(str))));
      return identifier;
    }
  }
  
  /**
   * Set the encryption key when asked by the client-side SASL object.
   */
  private static class SaslClientCallbackHandler implements CallbackHandler {
    
    private final byte[] encryptionKey;
    private final String userName;
    private final Token<BlockTokenIdentifier> blockToken;
    private final DatanodeID datanodeId;
    private final long timestamp;
    
    public SaslClientCallbackHandler(byte[] encryptionKey,
        Token<BlockTokenIdentifier> blockToken, DatanodeID datanodeId,
        String userName, long timestamp) {
      this.encryptionKey = encryptionKey;
      this.userName = userName;
      this.blockToken = blockToken;
      this.datanodeId = datanodeId;
      this.timestamp = timestamp;
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
        pc.setPassword(buildClientPassword());
      }
      if (rc != null) {
        rc.setText(rc.getDefaultText());
      }
    }

    /**
     * Calculates the password on the client side.  The password consists of the
     * block access token's password, the target DataNode UUID, and a
     * client-generated request timestamp.
     *
     * @return char[] client password
     */    
    private char[] buildClientPassword() {
      // TOOD: probably want to include block pool ID in password too
      return (new String(Base64.encodeBase64(blockToken.getPassword(), false),
        Charsets.UTF_8) + NAME_DELIMITER + datanodeId.getDatanodeUuid() +
        NAME_DELIMITER + timestamp).toCharArray();
    }
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
   * Given a secret manager and a username encoded as described above, determine
   * the encryption key.
   * 
   * @param blockPoolTokenSecretManager to determine the encryption key.
   * @param userName containing the keyId, blockPoolId, and nonce.
   * @return secret encryption key.
   * @throws IOException
   */
  private static byte[] getEncryptionKeyFromUserName(
      BlockPoolTokenSecretManager blockPoolTokenSecretManager, String userName)
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
  
  private static char[] encryptionKeyToPassword(byte[] encryptionKey) {
    return new String(Base64.encodeBase64(encryptionKey, false), Charsets.UTF_8).toCharArray();
  }
}
