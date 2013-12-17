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

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;

/**
 * Tests covering basic functionality of the Acl object.
 */
public class TestAcl {
  private static final Acl ACL1, ACL2, ACL3, ACL4;
  private static final AclEntry ENTRY1, ENTRY2, ENTRY3, ENTRY4, ENTRY5, ENTRY6,
    ENTRY7, ENTRY8, ENTRY9, ENTRY10, ENTRY11, ENTRY12;

  static {
    // named user
    AclEntry.Builder aclEntryBuilder = new AclEntry.Builder()
      .setType(AclEntryType.USER)
      .setName("user1")
      .setPermission(FsAction.ALL);
    ENTRY1 = aclEntryBuilder.build();
    // named group
    ENTRY2 = new AclEntry.Builder()
      .setType(AclEntryType.GROUP)
      .setName("group2")
      .setPermission(FsAction.READ_WRITE)
      .build();
    // default other
    ENTRY3 = new AclEntry.Builder()
      .setType(AclEntryType.OTHER)
      .setPermission(FsAction.NONE)
      .setScope(AclEntryScope.DEFAULT)
      .build();
    // owner
    ENTRY4 = new AclEntry.Builder()
      .setType(AclEntryType.USER)
      .setPermission(FsAction.ALL)
      .build();
    // default named group
    ENTRY5 = new AclEntry.Builder()
      .setType(AclEntryType.GROUP)
      .setName("group3")
      .setPermission(FsAction.READ_WRITE)
      .setScope(AclEntryScope.DEFAULT)
      .build();
    // other
    ENTRY6 = new AclEntry.Builder()
      .setType(AclEntryType.OTHER)
      .setPermission(FsAction.NONE)
      .build();
    // default named user
    ENTRY7 = new AclEntry.Builder()
      .setType(AclEntryType.USER)
      .setName("user3")
      .setPermission(FsAction.ALL)
      .setScope(AclEntryScope.DEFAULT)
      .build();
    // mask
    ENTRY8 = new AclEntry.Builder()
      .setType(AclEntryType.MASK)
      .setPermission(FsAction.READ)
      .build();
    // default mask
    ENTRY9 = new AclEntry.Builder()
      .setType(AclEntryType.MASK)
      .setPermission(FsAction.READ_EXECUTE)
      .setScope(AclEntryScope.DEFAULT)
      .build();
    // group
    ENTRY10 = new AclEntry.Builder()
      .setType(AclEntryType.GROUP)
      .setPermission(FsAction.READ)
      .build();
    // default group
    ENTRY11 = new AclEntry.Builder()
      .setType(AclEntryType.GROUP)
      .setPermission(FsAction.READ)
      .setScope(AclEntryScope.DEFAULT)
      .build();
    // default owner
    ENTRY12 = new AclEntry.Builder()
      .setType(AclEntryType.USER)
      .setPermission(FsAction.ALL)
      .setScope(AclEntryScope.DEFAULT)
      .build();

    Acl.Builder aclBuilder = new Acl.Builder()
      .addEntry(ENTRY1)
      .addEntry(ENTRY2)
      .addEntry(ENTRY3);
    ACL1 = aclBuilder.build();
    ACL2 = aclBuilder.build();
    ACL3 = new Acl.Builder()
      .setStickyBit(true)
      .build();

    ACL4 = new Acl.Builder()
      .addEntry(ENTRY1)
      .addEntry(ENTRY2)
      .addEntry(ENTRY3)
      .addEntry(ENTRY4)
      .addEntry(ENTRY5)
      .addEntry(ENTRY6)
      .addEntry(ENTRY7)
      .addEntry(ENTRY8)
      .addEntry(ENTRY9)
      .addEntry(ENTRY10)
      .addEntry(ENTRY11)
      .addEntry(ENTRY12)
      .build();
  }

  @Test
  public void testAclEquals() {
    assertNotSame(ACL1, ACL2);
    assertNotSame(ACL1, ACL3);
    assertNotSame(ACL2, ACL3);
    assertEquals(ACL1, ACL1);
    assertEquals(ACL2, ACL2);
    assertEquals(ACL1, ACL2);
    assertEquals(ACL2, ACL1);
    assertFalse(ACL1.equals(ACL3));
    assertFalse(ACL2.equals(ACL3));
    assertFalse(ACL1.equals(null));
    assertFalse(ACL1.equals(new Object()));
    Acl.Builder acl1WithDifferentStickyBit = new Acl.Builder();
    for (AclEntry entry: ACL1.getEntries()) {
      acl1WithDifferentStickyBit.addEntry(entry);
    }
    acl1WithDifferentStickyBit.setStickyBit(!ACL1.getStickyBit());
    assertFalse(ACL1.equals(acl1WithDifferentStickyBit.build()));
  }

  @Test
  public void testAclHashCode() {
    assertEquals(ACL1.hashCode(), ACL2.hashCode());
    assertFalse(ACL1.hashCode() == ACL3.hashCode());
  }

  @Test
  public void testAclEntriesImmutable() {
    AclEntry entry = new AclEntry.Builder().build();
    List<AclEntry> entries = ACL1.getEntries();
    try {
      entries.add(entry);
      fail("expected adding ACL entry to fail");
    } catch (UnsupportedOperationException e) {
      // expected
    }
  }

  @Test
  public void testEntryNaturalOrdering() {
    AclEntry expected[] = new AclEntry[] {
      ENTRY4,  // owner
      ENTRY1,  // named user
      ENTRY10, // group
      ENTRY2,  // named group
      ENTRY8,  // mask
      ENTRY6,  // other
      ENTRY12, // default owner
      ENTRY7,  // default named user
      ENTRY11, // default group
      ENTRY5,  // default named group
      ENTRY9,  // default mask
      ENTRY3   // default other
    };
    List<AclEntry> actual = ACL4.getEntries();
    assertNotNull(actual);
    assertArrayEquals(expected, actual.toArray(new AclEntry[actual.size()]));
  }

  @Test
  public void testToString() {
    assertEquals(
      "entries: [user:user1:rwx, group:group2:rw-, default:other::---], stickyBit: false",
      ACL1.toString());
    assertEquals(
      "entries: [user:user1:rwx, group:group2:rw-, default:other::---], stickyBit: false",
      ACL2.toString());
    assertEquals("entries: [], stickyBit: true", ACL3.toString());
  }
}
