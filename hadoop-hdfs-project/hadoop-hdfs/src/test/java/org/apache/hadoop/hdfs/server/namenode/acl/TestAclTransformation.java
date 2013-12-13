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
package org.apache.hadoop.hdfs.server.namenode.acl;

import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.namenode.acl.Acl;
import org.apache.hadoop.hdfs.server.namenode.acl.AclTransformation;

public class TestAclTransformation {

  @Test
  public void testNamedUser() {
    Acl existing = new Acl.Builder()
      .addEntry(aclEntry(ACCESS, USER, ALL))
      .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
      .addEntry(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Arrays.asList(
      aclEntry(ACCESS, USER, "joe", ALL));
    Acl expected = new Acl.Builder()
      .addEntry(aclEntry(ACCESS, USER, ALL))
      .addEntry(aclEntry(ACCESS, USER, "joe", ALL))
      .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
      .addEntry(aclEntry(ACCESS, MASK, ALL))
      .addEntry(aclEntry(ACCESS, OTHER, NONE))
      .build();
    Acl modified = AclTransformation.mergeAclEntries(aclSpec).apply(existing);
    assertEquals(expected, modified);
  }

  private static AclEntry aclEntry(AclEntryScope scope, AclEntryType type,
      FsAction permission) {
    return new AclEntry.Builder()
      .setScope(scope)
      .setType(type)
      .setPermission(permission)
      .build();
  }

  private static AclEntry aclEntry(AclEntryScope scope, AclEntryType type,
      String name, FsAction permission) {
    return new AclEntry.Builder()
      .setScope(scope)
      .setType(type)
      .setName(name)
      .setPermission(permission)
      .build();
  }
}
