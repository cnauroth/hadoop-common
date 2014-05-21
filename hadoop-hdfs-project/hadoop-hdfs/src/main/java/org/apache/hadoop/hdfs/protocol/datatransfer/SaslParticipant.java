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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Map;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.SaslInputStream;
import org.apache.hadoop.security.SaslOutputStream;

/**
 * Strongly inspired by Thrift's TSaslTransport class.
 *
 * Used to abstract over the <code>SaslServer</code> and
 * <code>SaslClient</code> classes, which share a lot of their interface, but
 * unfortunately don't share a common superclass.
 */
@InterfaceAudience.Private
class SaslParticipant {

  // This has to be set as part of the SASL spec, but it don't matter for
  // our purposes, but may not be empty. It's sent over the wire, so use
  // a short string.
  private static final String SERVER_NAME = "0";
  private static final String PROTOCOL = "hdfs";
  private static final String MECHANISM = "DIGEST-MD5";

  // One of these will always be null.
  private final SaslServer saslServer;
  private final SaslClient saslClient;

  public static SaslParticipant createServerSaslParticipant(
      Map<String, String> saslProps, CallbackHandler callbackHandler)
      throws SaslException {
    return new SaslParticipant(Sasl.createSaslServer(MECHANISM,
      PROTOCOL, SERVER_NAME, saslProps, callbackHandler));
  }

  public static SaslParticipant createClientSaslParticipant(String userName,
      Map<String, String> saslProps, CallbackHandler callbackHandler)
      throws SaslException {
    return new SaslParticipant(Sasl.createSaslClient(new String[] { MECHANISM },
      userName, PROTOCOL, SERVER_NAME, saslProps, callbackHandler));
  }

  private SaslParticipant(SaslServer saslServer) {
    this.saslServer = saslServer;
    this.saslClient = null;
  }

  private SaslParticipant(SaslClient saslClient) {
    this.saslServer = null;
    this.saslClient = saslClient;
  }

  public byte[] evaluateChallengeOrResponse(byte[] challengeOrResponse)
      throws SaslException {
    if (saslClient != null) {
      return saslClient.evaluateChallenge(challengeOrResponse);
    } else {
      return saslServer.evaluateResponse(challengeOrResponse);
    }
  }

  public boolean isComplete() {
    if (saslClient != null)
      return saslClient.isComplete();
    else
      return saslServer.isComplete();
  }

  public boolean supportsConfidentiality() {
    String qop = null;
    if (saslClient != null) {
      qop = (String) saslClient.getNegotiatedProperty(Sasl.QOP);
    } else {
      qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
    }
    return qop != null && qop.equals("auth-conf");
  }

  // Return some input/output streams that will henceforth have their
  // communication encrypted.
  public IOStreamPair createEncryptedStreamPair(DataOutputStream out,
      DataInputStream in) {
    if (saslClient != null) {
      return new IOStreamPair(
          new SaslInputStream(in, saslClient),
          new SaslOutputStream(out, saslClient));
    } else {
      return new IOStreamPair(
          new SaslInputStream(in, saslServer),
          new SaslOutputStream(out, saslServer));
    }
  }
}
