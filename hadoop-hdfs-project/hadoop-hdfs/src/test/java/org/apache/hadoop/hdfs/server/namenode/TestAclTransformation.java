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

import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.server.namenode.AclTransformation;

/**
 * Tests operations that modify ACLs.  All tests in this suite have been
 * cross-validated against Linux setfacl/getfacl to check for consistency of the
 * HDFS implementation.
 */
public class TestAclTransformation {

  private static final List<AclEntry> ACL_SPEC_TOO_LARGE;
  static {
    ACL_SPEC_TOO_LARGE = Lists.newArrayListWithCapacity(33);
    for (int i = 0; i < 33; ++i) {
      ACL_SPEC_TOO_LARGE.add(aclEntry(ACCESS, USER, "user" + i, ALL));
    }
  }

  @Test
  public void testFilterAclEntriesByAclSpec() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
        .add(aclEntry(ACCESS, USER, "diana", READ_EXECUTE))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, GROUP, "sales", READ_EXECUTE))
        .add(aclEntry(ACCESS, GROUP, "execs", READ_WRITE))
        .add(aclEntry(ACCESS, MASK, ALL))
        .add(aclEntry(ACCESS, OTHER, READ))
        .build(),
      AclTransformation.filterAclEntriesByAclSpec(Arrays.asList(
        aclEntry(ACCESS, USER, "diana"),
        aclEntry(ACCESS, GROUP, "sales"))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, GROUP, "execs", READ_WRITE))
        .add(aclEntry(ACCESS, MASK, READ_WRITE))
        .add(aclEntry(ACCESS, OTHER, READ))
        .build());
  }

  @Test
  public void testFilterAclEntriesByAclSpecUnchanged() throws AclException {
    assertAclUnchanged(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", ALL))
        .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .add(aclEntry(ACCESS, GROUP, "sales", ALL))
        .add(aclEntry(ACCESS, MASK, ALL))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.filterAclEntriesByAclSpec(Arrays.asList(
        aclEntry(ACCESS, USER, "clark"),
        aclEntry(ACCESS, GROUP, "execs"))));
  }

  @Test
  public void testFilterAclEntriesByAclSpecAccessMaskCalculated() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ))
        .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, READ_WRITE))
        .add(aclEntry(ACCESS, OTHER, READ))
        .build(),
      AclTransformation.filterAclEntriesByAclSpec(Arrays.asList(
        aclEntry(ACCESS, USER, "diana"))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .build());
  }

  @Test
  public void testFilterAclEntriesByAclSpecDefaultMaskCalculated() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ))
        .add(aclEntry(DEFAULT, USER, "diana", READ_WRITE))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, READ_WRITE))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build(),
      AclTransformation.filterAclEntriesByAclSpec(Arrays.asList(
        aclEntry(DEFAULT, USER, "diana"))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, READ))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build());
  }

  @Test
  public void testFilterAclEntriesByAclSpecDefaultMaskPreserved() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ))
        .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, READ_WRITE))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "diana", ALL))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, READ))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build(),
      AclTransformation.filterAclEntriesByAclSpec(Arrays.asList(
        aclEntry(ACCESS, USER, "diana"))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "diana", ALL))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, READ))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build());
  }

  @Test
  public void testFilterAclEntriesByAclSpecAccessMaskPreserved() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ))
        .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ))
        .add(aclEntry(DEFAULT, USER, "diana", READ_WRITE))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, READ_WRITE))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build(),
      AclTransformation.filterAclEntriesByAclSpec(Arrays.asList(
        aclEntry(DEFAULT, USER, "diana"))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ))
        .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, READ))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build());
  }

  @Test
  public void testFilterAclEntriesByAclSpecAutomaticDefaultUser() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, READ_WRITE))
        .add(aclEntry(DEFAULT, USER, "bruce", READ))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, READ))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build(),
      AclTransformation.filterAclEntriesByAclSpec(Arrays.asList(
        aclEntry(DEFAULT, USER))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, READ))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build());
  }

  @Test
  public void testFilterAclEntriesByAclSpecAutomaticDefaultGroup() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, READ_WRITE))
        .add(aclEntry(DEFAULT, GROUP, READ_WRITE))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build(),
      AclTransformation.filterAclEntriesByAclSpec(Arrays.asList(
        aclEntry(DEFAULT, GROUP))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, READ_WRITE))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build());
  }

  @Test
  public void testFilterAclEntriesByAclSpecAutomaticDefaultOther() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, READ_WRITE))
        .add(aclEntry(DEFAULT, GROUP, READ_WRITE))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build(),
      AclTransformation.filterAclEntriesByAclSpec(Arrays.asList(
        aclEntry(DEFAULT, OTHER))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, READ_WRITE))
        .add(aclEntry(DEFAULT, GROUP, READ_WRITE))
        .add(aclEntry(DEFAULT, OTHER, READ))
        .build());
  }

  @Test
  public void testFilterAclEntriesByAclSpecEmptyAclSpec() throws AclException {
    assertAclUnchanged(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, ALL))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ_WRITE))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, ALL))
        .add(aclEntry(DEFAULT, OTHER, READ))
        .build(),
      AclTransformation.filterAclEntriesByAclSpec(Arrays.<AclEntry>asList()));
  }

  @Test
  public void testFilterAclEntriesByAclSpecRemoveAccessMaskRequired()
      throws AclException {
    assertAclExceptionThrown(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, ALL))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.filterAclEntriesByAclSpec(Arrays.asList(
        aclEntry(ACCESS, MASK))));
  }

  @Test
  public void testFilterAclEntriesByAclSpecRemoveDefaultMaskRequired()
      throws AclException {
    assertAclExceptionThrown(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, ALL))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build(),
      AclTransformation.filterAclEntriesByAclSpec(Arrays.asList(
        aclEntry(DEFAULT, MASK))));
  }

  @Test
  public void testFilterAclEntriesByAclSpecInputTooLarge() throws AclException {
    assertAclExceptionThrown(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.filterAclEntriesByAclSpec(ACL_SPEC_TOO_LARGE));
  }

  @Test
  public void testFilterDefaultAclEntries() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
        .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .add(aclEntry(ACCESS, GROUP, "sales", READ_EXECUTE))
        .add(aclEntry(ACCESS, MASK, ALL))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ_WRITE))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, GROUP, "sales", READ_EXECUTE))
        .add(aclEntry(DEFAULT, MASK, READ_WRITE))
        .add(aclEntry(DEFAULT, OTHER, READ_EXECUTE))
        .build(),
      AclTransformation.filterDefaultAclEntries(),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
        .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .add(aclEntry(ACCESS, GROUP, "sales", READ_EXECUTE))
        .add(aclEntry(ACCESS, MASK, ALL))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build());
  }

  @Test
  public void testFilterDefaultAclEntriesUnchanged() throws AclException {
    assertAclUnchanged(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", ALL))
        .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .add(aclEntry(ACCESS, GROUP, "sales", ALL))
        .add(aclEntry(ACCESS, MASK, ALL))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.filterDefaultAclEntries());
  }

  @Test
  public void testFilterExtendedAclEntries() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
        .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .add(aclEntry(ACCESS, GROUP, "sales", READ_EXECUTE))
        .add(aclEntry(ACCESS, MASK, ALL))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ_WRITE))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, GROUP, "sales", READ_EXECUTE))
        .add(aclEntry(DEFAULT, MASK, READ_WRITE))
        .add(aclEntry(DEFAULT, OTHER, READ_EXECUTE))
        .build(),
      AclTransformation.filterExtendedAclEntries(),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build());
  }

  @Test
  public void testFilterExtendedAclEntriesUnchanged() throws AclException {
    assertAclUnchanged(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.filterExtendedAclEntries());
  }

  @Test
  public void testMergeAclEntries() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.mergeAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, "bruce", ALL))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", ALL))
        .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .add(aclEntry(ACCESS, MASK, ALL))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build());
  }

  @Test
  public void testMergeAclEntriesUnchanged() throws AclException {
    assertAclUnchanged(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", ALL))
        .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .add(aclEntry(ACCESS, GROUP, "sales", ALL))
        .add(aclEntry(ACCESS, MASK, ALL))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", ALL))
        .add(aclEntry(DEFAULT, GROUP, READ_EXECUTE))
        .add(aclEntry(DEFAULT, GROUP, "sales", ALL))
        .add(aclEntry(DEFAULT, MASK, ALL))
        .add(aclEntry(DEFAULT, OTHER, NONE))
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
  public void testMergeAclEntriesMultipleNewBeforeExisting()
      throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "diana", READ))
        .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .add(aclEntry(ACCESS, MASK, READ_EXECUTE))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.mergeAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
        aclEntry(ACCESS, USER, "clark", READ_EXECUTE),
        aclEntry(ACCESS, USER, "diana", READ_EXECUTE))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ_EXECUTE))
        .add(aclEntry(ACCESS, USER, "clark", READ_EXECUTE))
        .add(aclEntry(ACCESS, USER, "diana", READ_EXECUTE))
        .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .add(aclEntry(ACCESS, MASK, READ_EXECUTE))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build());
  }

  @Test
  public void testMergeAclEntriesAccessMaskCalculated() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .build(),
      AclTransformation.mergeAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
        aclEntry(ACCESS, USER, "diana", READ))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ_EXECUTE))
        .add(aclEntry(ACCESS, USER, "diana", READ))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, READ_EXECUTE))
        .add(aclEntry(ACCESS, OTHER, READ))
        .build());
  }

  @Test
  public void testMergeAclEntriesDefaultMaskCalculated() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, READ))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build(),
      AclTransformation.mergeAclEntries(Arrays.asList(
        aclEntry(DEFAULT, USER, "bruce", READ_WRITE),
        aclEntry(DEFAULT, USER, "diana", READ_EXECUTE))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ_WRITE))
        .add(aclEntry(DEFAULT, USER, "diana", READ_EXECUTE))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, ALL))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build());
  }

  @Test
  public void testMergeAclEntriesDefaultMaskPreserved() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "diana", ALL))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, READ))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build(),
      AclTransformation.mergeAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, "diana", FsAction.READ_EXECUTE))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "diana", READ_EXECUTE))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, READ_EXECUTE))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "diana", ALL))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, READ))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build());
  }

  @Test
  public void testMergeAclEntriesAccessMaskPreserved() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ))
        .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ))
        .add(aclEntry(DEFAULT, USER, "diana", READ_WRITE))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, READ_WRITE))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build(),
      AclTransformation.mergeAclEntries(Arrays.asList(
        aclEntry(DEFAULT, USER, "diana", READ_EXECUTE))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ))
        .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ))
        .add(aclEntry(DEFAULT, USER, "diana", READ_EXECUTE))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, READ_EXECUTE))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build());
  }

  @Test
  public void testMergeAclEntriesAutomaticDefaultUser() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .build(),
      AclTransformation.mergeAclEntries(Arrays.asList(
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, OTHER, READ))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, GROUP, READ_EXECUTE))
        .add(aclEntry(DEFAULT, OTHER, READ))
        .build());
  }

  @Test
  public void testMergeAclEntriesAutomaticDefaultGroup() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .build(),
      AclTransformation.mergeAclEntries(Arrays.asList(
        aclEntry(DEFAULT, USER, READ_EXECUTE),
        aclEntry(DEFAULT, OTHER, READ))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, READ_EXECUTE))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, OTHER, READ))
        .build());
  }

  @Test
  public void testMergeAclEntriesAutomaticDefaultOther() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.mergeAclEntries(Arrays.asList(
        aclEntry(DEFAULT, USER, READ_EXECUTE),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .add(aclEntry(DEFAULT, USER, READ_EXECUTE))
        .add(aclEntry(DEFAULT, GROUP, READ_EXECUTE))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build());
  }

  @Test
  public void testMergeAclEntriesProvidedAccessMask() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.mergeAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
        aclEntry(ACCESS, MASK, ALL))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ_EXECUTE))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, ALL))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build());
  }

  @Test
  public void testMergeAclEntriesProvidedDefaultMask() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.mergeAclEntries(Arrays.asList(
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, GROUP, READ),
        aclEntry(DEFAULT, MASK, ALL),
        aclEntry(DEFAULT, OTHER, NONE))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, ALL))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build());
  }

  @Test
  public void testMergeAclEntriesEmptyAclSpec() throws AclException {
    assertAclUnchanged(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, ALL))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ_WRITE))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, ALL))
        .add(aclEntry(DEFAULT, OTHER, READ))
        .build(),
      AclTransformation.mergeAclEntries(Arrays.<AclEntry>asList()));
  }

  @Test
  public void testMergeAclEntriesInputTooLarge() throws AclException {
    assertAclExceptionThrown(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.mergeAclEntries(ACL_SPEC_TOO_LARGE));
  }

  @Test
  public void testMergeAclEntriesResultTooLarge() throws AclException {
    ImmutableList.Builder<AclEntry> aclBuilder =
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL));
    for (int i = 1; i <= 28; ++i) {
      aclBuilder.add(aclEntry(ACCESS, USER, "user" + i, READ));
    }
    aclBuilder
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, READ))
      .add(aclEntry(ACCESS, OTHER, NONE));
    assertAclExceptionThrown(aclBuilder.build(),
      AclTransformation.mergeAclEntries(Arrays.asList(
      aclEntry(ACCESS, USER, "bruce", READ))));
  }

  @Test
  public void testMergeAclEntriesDuplicateEntries() throws AclException {
    assertAclExceptionThrown(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.mergeAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, "bruce", ALL),
        aclEntry(ACCESS, USER, "diana", READ_WRITE),
        aclEntry(ACCESS, USER, "clark", READ),
        aclEntry(ACCESS, USER, "bruce", READ_EXECUTE))));
  }

  @Test
  public void testMergeAclEntriesNamedMask() throws AclException {
    assertAclExceptionThrown(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.mergeAclEntries(Arrays.asList(
        aclEntry(ACCESS, MASK, "bruce", READ_EXECUTE))));
  }

  @Test
  public void testMergeAclEntriesNamedOther() throws AclException {
    assertAclExceptionThrown(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.mergeAclEntries(Arrays.asList(
        aclEntry(ACCESS, OTHER, "bruce", READ_EXECUTE))));
  }

  @Test
  public void testReplaceAclEntries() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", ALL))
        .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .add(aclEntry(ACCESS, MASK, ALL))
        .add(aclEntry(ACCESS, OTHER, NONE))
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
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
        .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .add(aclEntry(ACCESS, GROUP, "sales", ALL))
        .add(aclEntry(ACCESS, MASK, ALL))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ_WRITE))
        .add(aclEntry(DEFAULT, GROUP, READ_EXECUTE))
        .add(aclEntry(DEFAULT, GROUP, "sales", ALL))
        .add(aclEntry(DEFAULT, MASK, ALL))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build());
  }

  @Test
  public void testReplaceAclEntriesUnchanged() throws AclException {
    assertAclUnchanged(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", ALL))
        .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
        .add(aclEntry(ACCESS, GROUP, "sales", ALL))
        .add(aclEntry(ACCESS, MASK, ALL))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", ALL))
        .add(aclEntry(DEFAULT, GROUP, READ_EXECUTE))
        .add(aclEntry(DEFAULT, GROUP, "sales", ALL))
        .add(aclEntry(DEFAULT, MASK, ALL))
        .add(aclEntry(DEFAULT, OTHER, NONE))
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
  public void testReplaceAclEntriesAccessMaskCalculated() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .build(),
      AclTransformation.replaceAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, "bruce", READ),
        aclEntry(ACCESS, USER, "diana", READ_WRITE),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, OTHER, READ))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ))
        .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, READ_WRITE))
        .add(aclEntry(ACCESS, OTHER, READ))
        .build());
  }

  @Test
  public void testReplaceAclEntriesDefaultMaskCalculated() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .build(),
      AclTransformation.replaceAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, OTHER, READ),
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, "bruce", READ),
        aclEntry(DEFAULT, USER, "diana", READ_WRITE),
        aclEntry(DEFAULT, GROUP, ALL),
        aclEntry(DEFAULT, OTHER, READ))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ))
        .add(aclEntry(DEFAULT, USER, "diana", READ_WRITE))
        .add(aclEntry(DEFAULT, GROUP, ALL))
        .add(aclEntry(DEFAULT, MASK, ALL))
        .add(aclEntry(DEFAULT, OTHER, READ))
        .build());
  }

  @Test
  public void testReplaceAclEntriesDefaultMaskPreserved() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ))
        .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, READ_WRITE))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "diana", ALL))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, READ))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build(),
      AclTransformation.replaceAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, "bruce", READ),
        aclEntry(ACCESS, USER, "diana", READ_WRITE),
        aclEntry(ACCESS, GROUP, ALL),
        aclEntry(ACCESS, OTHER, READ))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ))
        .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
        .add(aclEntry(ACCESS, GROUP, ALL))
        .add(aclEntry(ACCESS, MASK, ALL))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "diana", ALL))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, READ))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build());
  }

  @Test
  public void testReplaceAclEntriesAccessMaskPreserved() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ))
        .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ))
        .add(aclEntry(DEFAULT, USER, "diana", READ_WRITE))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, READ_WRITE))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build(),
      AclTransformation.replaceAclEntries(Arrays.asList(
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, "bruce", READ),
        aclEntry(DEFAULT, GROUP, READ),
        aclEntry(DEFAULT, OTHER, NONE))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, USER, "bruce", READ))
        .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, MASK, READ))
        .add(aclEntry(ACCESS, OTHER, READ))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, READ))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build());
  }

  @Test
  public void testReplaceAclEntriesAutomaticDefaultUser() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.replaceAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, "bruce", READ),
        aclEntry(DEFAULT, GROUP, READ_WRITE),
        aclEntry(DEFAULT, MASK, READ_WRITE),
        aclEntry(DEFAULT, OTHER, READ))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, USER, "bruce", READ))
        .add(aclEntry(DEFAULT, GROUP, READ_WRITE))
        .add(aclEntry(DEFAULT, MASK, READ_WRITE))
        .add(aclEntry(DEFAULT, OTHER, READ))
        .build());
  }

  @Test
  public void testReplaceAclEntriesAutomaticDefaultGroup() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.replaceAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, READ_WRITE),
        aclEntry(DEFAULT, USER, "bruce", READ),
        aclEntry(DEFAULT, MASK, READ),
        aclEntry(DEFAULT, OTHER, READ))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .add(aclEntry(DEFAULT, USER, READ_WRITE))
        .add(aclEntry(DEFAULT, USER, "bruce", READ))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, MASK, READ))
        .add(aclEntry(DEFAULT, OTHER, READ))
        .build());
  }

  @Test
  public void testReplaceAclEntriesAutomaticDefaultOther() throws AclException {
    assertAclModified(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.replaceAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, READ_WRITE),
        aclEntry(DEFAULT, USER, "bruce", READ),
        aclEntry(DEFAULT, GROUP, READ_WRITE),
        aclEntry(DEFAULT, MASK, READ_WRITE))),
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .add(aclEntry(DEFAULT, USER, READ_WRITE))
        .add(aclEntry(DEFAULT, USER, "bruce", READ))
        .add(aclEntry(DEFAULT, GROUP, READ_WRITE))
        .add(aclEntry(DEFAULT, MASK, READ_WRITE))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build());
  }

  @Test
  public void testReplaceAclEntriesInputTooLarge() throws AclException {
    assertAclExceptionThrown(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.replaceAclEntries(ACL_SPEC_TOO_LARGE));
  }

  @Test
  public void testReplaceAclEntriesResultTooLarge() throws AclException {
    List<AclEntry> aclSpec = Lists.newArrayListWithCapacity(32);
    aclSpec.add(aclEntry(ACCESS, USER, ALL));
    for (int i = 1; i <= 29; ++i) {
      aclSpec.add(aclEntry(ACCESS, USER, "user" + i, READ));
    }
    aclSpec.add(aclEntry(ACCESS, GROUP, READ));
    aclSpec.add(aclEntry(ACCESS, OTHER, NONE));
    // The ACL spec now has 32 entries.  Automatic mask calculation will push it
    // over the limit to 33.
    assertAclExceptionThrown(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.replaceAclEntries(aclSpec));
  }

  @Test
  public void testReplaceAclEntriesDuplicateEntries() throws AclException {
    assertAclExceptionThrown(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.replaceAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, "bruce", ALL),
        aclEntry(ACCESS, USER, "diana", READ_WRITE),
        aclEntry(ACCESS, USER, "clark", READ),
        aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, OTHER, NONE))));
  }

  @Test
  public void testReplaceAclEntriesNamedMask() throws AclException {
    assertAclExceptionThrown(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.replaceAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(ACCESS, MASK, "bruce", READ_EXECUTE))));
  }

  @Test
  public void testReplaceAclEntriesNamedOther() throws AclException {
    assertAclExceptionThrown(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.replaceAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(ACCESS, OTHER, "bruce", READ_EXECUTE))));
  }

  @Test
  public void testReplaceAclEntriesMissingUser() throws AclException {
    assertAclExceptionThrown(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.replaceAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, "bruce", READ_WRITE),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, GROUP, "sales", ALL),
        aclEntry(ACCESS, MASK, ALL),
        aclEntry(ACCESS, OTHER, NONE))));
  }

  @Test
  public void testReplaceAclEntriesMissingGroup() throws AclException {
    assertAclExceptionThrown(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.replaceAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, "bruce", READ_WRITE),
        aclEntry(ACCESS, GROUP, "sales", ALL),
        aclEntry(ACCESS, MASK, ALL),
        aclEntry(ACCESS, OTHER, NONE))));
  }

  @Test
  public void testReplaceAclEntriesMissingOther() throws AclException {
    assertAclExceptionThrown(
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL))
        .add(aclEntry(ACCESS, GROUP, READ))
        .add(aclEntry(ACCESS, OTHER, NONE))
        .build(),
      AclTransformation.replaceAclEntries(Arrays.asList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, "bruce", READ_WRITE),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, GROUP, "sales", ALL),
        aclEntry(ACCESS, MASK, ALL))));
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

  private static AclEntry aclEntry(AclEntryScope scope, AclEntryType type) {
    return new AclEntry.Builder()
      .setScope(scope)
      .setType(type)
      .build();
  }

  private static void assertAclModified(List<AclEntry> existing,
      AclTransformation transformation, List<AclEntry> expected)
      throws AclException {
    List<AclEntry> modified = transformation.apply(existing);
    assertEquals(expected, modified);
  }

  private static void assertAclUnchanged(List<AclEntry> existing,
      AclTransformation transformation) throws AclException {
    List<AclEntry> modified = transformation.apply(existing);
    assertEquals(existing, modified);
  }

  private static void assertAclExceptionThrown(List<AclEntry> existing,
      AclTransformation transformation) {
    try {
      List<AclEntry> modified = transformation.apply(existing);
      fail("Expected AclException, but received modified ACL: " + modified);
    } catch (AclException e) {
      // expected
    }
  }
}
