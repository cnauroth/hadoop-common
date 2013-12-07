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
package org.apache.hadoop.fs.permission;

import static org.junit.Assert.*;

import java.util.Set;

import org.junit.Test;

import org.apache.hadoop.fs.Path;

/**
 * Tests covering basic functionality of the ACL objects.
 */
public class TestAcl {
  private static final Acl ACL1, ACL2, ACL3;
  private static final AclEntry ENTRY1, ENTRY2, ENTRY3, ENTRY4;
  private static final AclStatus STATUS1, STATUS2, STATUS3;

  static {
    AclEntry.Builder aclEntryBuilder = new AclEntry.Builder()
      .setType(AclEntryType.USER)
      .setName("user1")
      .setPermission(FsAction.ALL);
    ENTRY1 = aclEntryBuilder.build();
    ENTRY2 = aclEntryBuilder.build();
    ENTRY3 = new AclEntry.Builder()
      .setType(AclEntryType.GROUP)
      .setName("group2")
      .setPermission(FsAction.READ_WRITE)
      .build();
    ENTRY4 = new AclEntry.Builder()
      .setType(AclEntryType.OTHER)
      .setPermission(FsAction.NONE)
      .setScope(AclEntryScope.DEFAULT)
      .build();

    Acl.Builder aclBuilder = new Acl.Builder()
      .addEntry(ENTRY1)
      .addEntry(ENTRY3)
      .addEntry(ENTRY4);
    ACL1 = aclBuilder.build();
    ACL2 = aclBuilder.build();
    ACL3 = new Acl.Builder().build();

    AclStatus.Builder aclStatusBuilder = new AclStatus.Builder()
      .setFile(new Path("file1"))
      .setOwner("owner1")
      .setGroup("group1")
      .setAcl(ACL1);
    STATUS1 = aclStatusBuilder.build();
    STATUS2 = aclStatusBuilder.build();
    STATUS3 = new AclStatus.Builder()
      .setFile(new Path("file2"))
      .setOwner("owner2")
      .setGroup("group2")
      .setAcl(ACL3)
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
  }

  @Test
  public void testAclHashCode() {
    assertEquals(ACL1.hashCode(), ACL2.hashCode());
    assertFalse(ACL1.hashCode() == ACL3.hashCode());
  }

  @Test
  public void testAclEntriesImmutable() {
    AclEntry entry = new AclEntry.Builder().build();
    Set<AclEntry> entries = ACL1.getEntries();
    try {
      entries.add(entry);
      fail("expected adding ACL entry to fail");
    } catch (UnsupportedOperationException e) {
      // expected
    }
  }

  @Test
  public void testEntryEquals() {
    assertNotSame(ENTRY1, ENTRY2);
    assertNotSame(ENTRY1, ENTRY3);
    assertNotSame(ENTRY1, ENTRY4);
    assertNotSame(ENTRY2, ENTRY3);
    assertNotSame(ENTRY2, ENTRY4);
    assertNotSame(ENTRY3, ENTRY4);
    assertEquals(ENTRY1, ENTRY1);
    assertEquals(ENTRY2, ENTRY2);
    assertEquals(ENTRY1, ENTRY2);
    assertEquals(ENTRY2, ENTRY1);
    assertFalse(ENTRY1.equals(ENTRY3));
    assertFalse(ENTRY1.equals(ENTRY4));
    assertFalse(ENTRY3.equals(ENTRY4));
    assertFalse(ENTRY1.equals(null));
    assertFalse(ENTRY1.equals(new Object()));
  }

  @Test
  public void testEntryHashCode() {
    assertEquals(ENTRY1.hashCode(), ENTRY2.hashCode());
    assertFalse(ENTRY1.hashCode() == ENTRY3.hashCode());
    assertFalse(ENTRY1.hashCode() == ENTRY4.hashCode());
    assertFalse(ENTRY3.hashCode() == ENTRY4.hashCode());
  }

  @Test
  public void testEntryScopeIsAccessIfUnspecified() {
    assertEquals(AclEntryScope.ACCESS, ENTRY1.getScope());
    assertEquals(AclEntryScope.ACCESS, ENTRY2.getScope());
    assertEquals(AclEntryScope.ACCESS, ENTRY3.getScope());
    assertEquals(AclEntryScope.DEFAULT, ENTRY4.getScope());
  }

  @Test
  public void testStatusEquals() {
    assertNotSame(STATUS1, STATUS2);
    assertNotSame(STATUS1, STATUS3);
    assertNotSame(STATUS2, STATUS3);
    assertEquals(STATUS1, STATUS1);
    assertEquals(STATUS2, STATUS2);
    assertEquals(STATUS1, STATUS2);
    assertEquals(STATUS2, STATUS1);
    assertFalse(STATUS1.equals(STATUS3));
    assertFalse(STATUS2.equals(STATUS3));
    assertFalse(STATUS1.equals(null));
    assertFalse(STATUS1.equals(new Object()));
  }

  @Test
  public void testStatusHashCode() {
    assertEquals(STATUS1.hashCode(), STATUS2.hashCode());
    assertFalse(STATUS1.hashCode() == STATUS3.hashCode());
  }

  @Test
  public void testToString() {
    assertEquals(
      "entries: [user:user1:rwx, group:group2:rw-, default:other::---], stickyBit: false",
      ACL1.toString());
    assertEquals(
      "entries: [user:user1:rwx, group:group2:rw-, default:other::---], stickyBit: false",
      ACL2.toString());
    assertEquals("entries: [], stickyBit: false", ACL3.toString());
    assertEquals("user:user1:rwx", ENTRY1.toString());
    assertEquals("user:user1:rwx", ENTRY2.toString());
    assertEquals("group:group2:rw-", ENTRY3.toString());
    assertEquals("default:other::---", ENTRY4.toString());
    assertEquals(
      "file: file1, owner: owner1, group: group1, acl: {entries: [user:user1:rwx, group:group2:rw-, default:other::---], stickyBit: false}",
      STATUS1.toString());
    assertEquals(
      "file: file1, owner: owner1, group: group1, acl: {entries: [user:user1:rwx, group:group2:rw-, default:other::---], stickyBit: false}",
      STATUS2.toString());
    assertEquals(
      "file: file2, owner: owner2, group: group2, acl: {entries: [], stickyBit: false}",
      STATUS3.toString());
  }
}
