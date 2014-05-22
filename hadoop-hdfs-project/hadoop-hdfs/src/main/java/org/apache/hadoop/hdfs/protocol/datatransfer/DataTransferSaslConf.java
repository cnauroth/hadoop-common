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
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_TRANSFER_SASL_PROPS_RESOLVER_CLASS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SaslPropertiesResolver;

@InterfaceAudience.Private
public class DataTransferSaslConf {

  final boolean encryptDataTransfer;
  final String encryptionAlgorithm;
  final SaslPropertiesResolver saslPropsResolver;
  final TrustedChannelResolver trustedChannelResolver;

  public DataTransferSaslConf(Configuration conf) {
    this.encryptDataTransfer = conf.getBoolean(DFS_ENCRYPT_DATA_TRANSFER_KEY,
      DFS_ENCRYPT_DATA_TRANSFER_DEFAULT);
    this.encryptionAlgorithm = conf.get(DFS_DATA_ENCRYPTION_ALGORITHM_KEY);

    Configuration saslPropsResolverConf = new Configuration(conf);
    saslPropsResolverConf.set(HADOOP_RPC_PROTECTION,
      conf.get(DFS_DATA_TRANSFER_PROTECTION_KEY,
        DFS_DATA_TRANSFER_PROTECTION_DEFAULT));
    saslPropsResolverConf.setClass(HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS,
      conf.getClass(DFS_DATA_TRANSFER_SASL_PROPS_RESOLVER_CLASS_KEY,
        SaslPropertiesResolver.class), SaslPropertiesResolver.class);
    this.saslPropsResolver = SaslPropertiesResolver.getInstance(conf);

    this.trustedChannelResolver = TrustedChannelResolver.getInstance(conf);
  }
}
