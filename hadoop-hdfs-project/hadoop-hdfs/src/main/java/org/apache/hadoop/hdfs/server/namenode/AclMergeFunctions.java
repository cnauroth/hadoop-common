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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.Acl;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;

@InterfaceAudience.LimitedPrivate({"HDFS"})
final class AclMergeFunctions {

  public static Function<Acl, Acl> mergeRemoveAcl() {
    return MERGE_REMOVE_ACL;
  }
  private static final Function<Acl, Acl> MERGE_REMOVE_ACL =
    new Function<Acl, Acl>() {
      @Override
      public Acl apply(Acl existingAcl) {
        Acl.Builder aclBuilder = startAclBuilder(existingAcl);
        for (AclEntry existingEntry: existingAcl.getEntries()) {
          if (existingEntry.getScope() == AclEntryScope.ACCESS &&
              existingEntry.getType() != AclEntryType.MASK &&
              existingEntry.getName() == null) {
            aclBuilder.addEntry(existingEntry);
          }
        }
        return aclBuilder.build();
      }
    };

  public static Function<Acl, Acl> mergeRemoveAclEntries(
      final List<AclEntry> aclSpec) {
    return new Function<Acl, Acl>() {
      @Override
      public Acl apply(Acl existingAcl) {
        Acl.Builder aclBuilder = startAclBuilder(existingAcl);
        Iterator<AclEntry> aclSpecIter = aclSpec.iterator();
        AclEntry aclSpecEntry = null;
        for (AclEntry existingEntry: existingAcl.getEntries()) {
          while (aclSpecIter.hasNext() && (aclSpecEntry == null ||
              aclSpecEntry.compareTo(existingEntry) < 0)) {
            aclSpecEntry = aclSpecIter.next();
          }
          if (existingEntry.compareTo(aclSpecEntry) != 0) {
            aclBuilder.addEntry(existingEntry);
          }
        }
        return aclBuilder.build();
      }
    };
  }

  /**
   * There is no reason to instantiate this class.
   */
  private AclMergeFunctions() {
  }

  private static Acl.Builder startAclBuilder(Acl existingAcl) {
    return new Acl.Builder().setStickyBit(existingAcl.getStickyBit());
  }
}
