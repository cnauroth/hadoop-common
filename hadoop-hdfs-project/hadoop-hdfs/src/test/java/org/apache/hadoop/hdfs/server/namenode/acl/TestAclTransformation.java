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
  public void testFilterAclEntriesByAclSpec() {
    assertAclModified(
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
        .addEntry(aclEntry(ACCESS, USER, "diana", READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, GROUP, READ))
        .addEntry(aclEntry(ACCESS, GROUP, "sales", READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, GROUP, "execs", READ_WRITE))
        .addEntry(aclEntry(ACCESS, MASK, ALL))
        .addEntry(aclEntry(ACCESS, OTHER, READ))
        .build(),
      AclTransformation.filterAclEntriesByAclSpec(Arrays.asList(
        aclEntry(ACCESS, USER, "diana"),
        aclEntry(ACCESS, GROUP, "sales"))),
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
        .addEntry(aclEntry(ACCESS, GROUP, READ))
        .addEntry(aclEntry(ACCESS, GROUP, "execs", READ_WRITE))
        .addEntry(aclEntry(ACCESS, MASK, READ_WRITE))
        .addEntry(aclEntry(ACCESS, OTHER, READ))
        .build());

    assertAclUnchanged(
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", ALL))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, GROUP, "sales", ALL))
        .addEntry(aclEntry(ACCESS, MASK, ALL))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.filterAclEntriesByAclSpec(Arrays.asList(
        aclEntry(ACCESS, USER, "clark"),
        aclEntry(ACCESS, GROUP, "execs"))));
  }

  @Test
  public void testFilterDefaultAclEntries() {
    assertAclModified(
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, GROUP, "sales", READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, MASK, ALL))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .addEntry(aclEntry(DEFAULT, USER, ALL))
        .addEntry(aclEntry(DEFAULT, USER, "bruce", READ_WRITE))
        .addEntry(aclEntry(DEFAULT, GROUP, READ))
        .addEntry(aclEntry(DEFAULT, GROUP, "sales", READ_EXECUTE))
        .addEntry(aclEntry(DEFAULT, MASK, READ_WRITE))
        .addEntry(aclEntry(DEFAULT, OTHER, READ_EXECUTE))
        .build(),
      AclTransformation.filterDefaultAclEntries(),
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, GROUP, "sales", READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, MASK, ALL))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .build());

    assertAclUnchanged(
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", ALL))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, GROUP, "sales", ALL))
        .addEntry(aclEntry(ACCESS, MASK, ALL))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.filterDefaultAclEntries());
  }

  @Test
  public void testFilterExtendedAclEntries() {
    assertAclModified(
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, GROUP, "sales", READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, MASK, ALL))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .addEntry(aclEntry(DEFAULT, USER, ALL))
        .addEntry(aclEntry(DEFAULT, USER, "bruce", READ_WRITE))
        .addEntry(aclEntry(DEFAULT, GROUP, READ))
        .addEntry(aclEntry(DEFAULT, GROUP, "sales", READ_EXECUTE))
        .addEntry(aclEntry(DEFAULT, MASK, READ_WRITE))
        .addEntry(aclEntry(DEFAULT, OTHER, READ_EXECUTE))
        .build(),
      AclTransformation.filterExtendedAclEntries(),
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .build());

    assertAclUnchanged(
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.filterExtendedAclEntries());
  }

  @Test
  public void testMergeAclEntries() {
    assertAclModified(
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.mergeAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, "bruce", ALL))),
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", ALL))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, MASK, ALL))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .build());

    assertAclUnchanged(
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", ALL))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, GROUP, "sales", ALL))
        .addEntry(aclEntry(ACCESS, MASK, ALL))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .addEntry(aclEntry(DEFAULT, USER, ALL))
        .addEntry(aclEntry(DEFAULT, USER, "bruce", ALL))
        .addEntry(aclEntry(DEFAULT, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(DEFAULT, GROUP, "sales", ALL))
        .addEntry(aclEntry(DEFAULT, MASK, ALL))
        .addEntry(aclEntry(DEFAULT, OTHER, NONE))
        .build(),
      AclTransformation.mergeAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, "bruce", ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, GROUP, "sales", ALL),
        aclEntry(ACCESS, MASK, ALL),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, "bruce", ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, GROUP, "sales", ALL),
        aclEntry(DEFAULT, MASK, ALL),
        aclEntry(DEFAULT, OTHER, NONE))));
  }

  @Test
  public void testReplaceAclEntries() {
    assertAclModified(
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", ALL))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, MASK, ALL))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.replaceAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, "bruce", READ_WRITE),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, GROUP, "sales", ALL),
        aclEntry(ACCESS, MASK, ALL),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, "bruce", READ_WRITE),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, GROUP, "sales", ALL),
        aclEntry(DEFAULT, MASK, ALL),
        aclEntry(DEFAULT, OTHER, NONE))),
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, GROUP, "sales", ALL))
        .addEntry(aclEntry(ACCESS, MASK, ALL))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .addEntry(aclEntry(DEFAULT, USER, ALL))
        .addEntry(aclEntry(DEFAULT, USER, "bruce", READ_WRITE))
        .addEntry(aclEntry(DEFAULT, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(DEFAULT, GROUP, "sales", ALL))
        .addEntry(aclEntry(DEFAULT, MASK, ALL))
        .addEntry(aclEntry(DEFAULT, OTHER, NONE))
        .build());

    assertAclUnchanged(
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", ALL))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, GROUP, "sales", ALL))
        .addEntry(aclEntry(ACCESS, MASK, ALL))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .addEntry(aclEntry(DEFAULT, USER, ALL))
        .addEntry(aclEntry(DEFAULT, USER, "bruce", ALL))
        .addEntry(aclEntry(DEFAULT, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(DEFAULT, GROUP, "sales", ALL))
        .addEntry(aclEntry(DEFAULT, MASK, ALL))
        .addEntry(aclEntry(DEFAULT, OTHER, NONE))
        .build(),
      AclTransformation.replaceAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, "bruce", ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, GROUP, "sales", ALL),
        aclEntry(ACCESS, MASK, ALL),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, "bruce", ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, GROUP, "sales", ALL),
        aclEntry(DEFAULT, MASK, ALL),
        aclEntry(DEFAULT, OTHER, NONE))));
  }

  @Test
  public void testPreserveStickyBit() {
    assertAclModified(
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
        .addEntry(aclEntry(ACCESS, USER, "diana", READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, GROUP, READ))
        .addEntry(aclEntry(ACCESS, MASK, ALL))
        .addEntry(aclEntry(ACCESS, OTHER, READ))
        .setStickyBit(true)
        .build(),
      AclTransformation.filterAclEntriesByAclSpec(Arrays.asList(
        aclEntry(ACCESS, USER, "diana"))),
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
        .addEntry(aclEntry(ACCESS, GROUP, READ))
        .addEntry(aclEntry(ACCESS, MASK, READ_WRITE))
        .addEntry(aclEntry(ACCESS, OTHER, READ))
        .setStickyBit(true)
        .build());

    assertAclModified(
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .addEntry(aclEntry(DEFAULT, USER, ALL))
        .addEntry(aclEntry(DEFAULT, USER, "bruce", READ_WRITE))
        .addEntry(aclEntry(DEFAULT, GROUP, READ))
        .addEntry(aclEntry(DEFAULT, MASK, READ_WRITE))
        .addEntry(aclEntry(DEFAULT, OTHER, READ_EXECUTE))
        .setStickyBit(true)
        .build(),
      AclTransformation.filterDefaultAclEntries(),
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .setStickyBit(true)
        .build());

    assertAclModified(
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, GROUP, "sales", READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, MASK, ALL))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .setStickyBit(true)
        .build(),
      AclTransformation.filterExtendedAclEntries(),
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .setStickyBit(true)
        .build());

    assertAclModified(
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .setStickyBit(true)
        .build(),
      AclTransformation.mergeAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, "bruce", ALL))),
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", ALL))
        .addEntry(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .addEntry(aclEntry(ACCESS, MASK, ALL))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .setStickyBit(true)
        .build());

    assertAclModified(
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, GROUP, READ_WRITE))
        .addEntry(aclEntry(ACCESS, GROUP, "sales", READ))
        .addEntry(aclEntry(ACCESS, MASK, READ_WRITE))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .setStickyBit(true)
        .build(),
      AclTransformation.replaceAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, "bruce", READ_WRITE),
        aclEntry(ACCESS, GROUP, READ_WRITE),
        aclEntry(ACCESS, GROUP, "sales", READ_WRITE),
        aclEntry(ACCESS, MASK, READ_WRITE),
        aclEntry(ACCESS, OTHER, NONE))),
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
        .addEntry(aclEntry(ACCESS, GROUP, READ_WRITE))
        .addEntry(aclEntry(ACCESS, GROUP, "sales", READ_WRITE))
        .addEntry(aclEntry(ACCESS, MASK, READ_WRITE))
        .addEntry(aclEntry(ACCESS, OTHER, NONE))
        .setStickyBit(true)
        .build());
  }

  @Test
  public void testMaskCalculation() {
    assertAclModified(
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", READ))
        .addEntry(aclEntry(ACCESS, USER, "diana", READ_WRITE))
        .addEntry(aclEntry(ACCESS, GROUP, READ))
        .addEntry(aclEntry(ACCESS, MASK, READ_WRITE))
        .addEntry(aclEntry(ACCESS, OTHER, READ))
        .build(),
      AclTransformation.filterAclEntriesByAclSpec(Arrays.asList(
        aclEntry(ACCESS, USER, "diana"))),
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", READ))
        .addEntry(aclEntry(ACCESS, GROUP, READ))
        .addEntry(aclEntry(ACCESS, MASK, READ))
        .addEntry(aclEntry(ACCESS, OTHER, READ))
        .build());

    assertAclModified(
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", READ))
        .addEntry(aclEntry(ACCESS, USER, "diana", READ_WRITE))
        .addEntry(aclEntry(ACCESS, GROUP, READ))
        .addEntry(aclEntry(ACCESS, MASK, READ_WRITE))
        .addEntry(aclEntry(ACCESS, OTHER, READ))
        .addEntry(aclEntry(DEFAULT, USER, ALL))
        .addEntry(aclEntry(DEFAULT, USER, "diana", ALL))
        .addEntry(aclEntry(DEFAULT, GROUP, READ))
        .addEntry(aclEntry(DEFAULT, MASK, READ))
        .addEntry(aclEntry(DEFAULT, OTHER, NONE))
        .build(),
      AclTransformation.filterAclEntriesByAclSpec(Arrays.asList(
        aclEntry(ACCESS, USER, "diana"))),
      new Acl.Builder()
        .addEntry(aclEntry(ACCESS, USER, ALL))
        .addEntry(aclEntry(ACCESS, USER, "bruce", READ))
        .addEntry(aclEntry(ACCESS, GROUP, READ))
        .addEntry(aclEntry(ACCESS, MASK, READ))
        .addEntry(aclEntry(ACCESS, OTHER, READ))
        .addEntry(aclEntry(DEFAULT, USER, ALL))
        .addEntry(aclEntry(DEFAULT, USER, "diana", ALL))
        .addEntry(aclEntry(DEFAULT, GROUP, READ))
        .addEntry(aclEntry(DEFAULT, MASK, READ))
        .addEntry(aclEntry(DEFAULT, OTHER, NONE))
        .build());
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

  private static AclEntry aclEntry(AclEntryScope scope, AclEntryType type,
      String name) {
    return new AclEntry.Builder()
      .setScope(scope)
      .setType(type)
      .setName(name)
      .build();
  }

  private static void assertAclModified(Acl existing,
      AclTransformation transformation, Acl expected) {
    Acl modified = transformation.apply(existing);
    assertEquals(expected, modified);
  }

  private static void assertAclUnchanged(Acl existing,
      AclTransformation transformation) {
    Acl modified = transformation.apply(existing);
    assertEquals(existing, modified);
  }
}
