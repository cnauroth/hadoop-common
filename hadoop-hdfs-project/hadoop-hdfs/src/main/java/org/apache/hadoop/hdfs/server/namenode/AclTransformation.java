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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.AclException;

/**
 * AclTransformation defines the operations that can modify an ACL.  All ACL
 * modifications take as input an existing ACL and apply logic to add new
 * entries, modify existing entries or remove old entries.  Some operations also
 * accept an ACL spec: a list of entries that further describes the requested
 * change.  Different operations interpret the ACL spec differently.  In the
 * case of adding an ACL to an inode that previously did not have one, the
 * existing ACL can be a "minimal ACL" containing exactly 3 entries for owner,
 * group and other, all derived from the {@link FsPermission} bits.
 *
 * The algorithms implemented here require sorted lists of ACL entries.  For any
 * existing ACL, it is assumed that the entries are sorted.  This is because all
 * ACL creation and modification is intended to go through these methods, and
 * they all guarantee correct sort order in their outputs due to explicit
 * sorting and reliance on the natural ordering defined in
 * {@link AclEntry#compareTo}.  However, an ACL spec is considered untrusted
 * user input, so all operations pre-sort the ACL spec as the first step.
 *
 * {@link #filterAclEntriesByAclSpec}, {@link #mergeAclEntries} and
 * {@link #replaceAclEntries} all have a lot in common in terms of needing to
 * track state for validation, mask calculations and automatic creation of
 * default entries if not provided.  Each of these methods delegates to two
 * internal helper classes to manage the required state tracking:
 * {@link #AclSpecTransformationState} and {@link #MaskCalculator}.  For
 * {@link #filterDefaultAclEntries}, this state tracking is not required, so
 * this method does not make use of the helper classes.
 */
@InterfaceAudience.Private
final class AclTransformation {
  private static final int MAX_ENTRIES = 32;

  /**
   * Filters (discards) any existing ACL entries that have the same scope, type
   * and name of any entry in the ACL spec.  If necessary, recalculates the mask
   * entries.  If necessary, default entries may be inferred by copying the
   * permissions of the corresponding access entries.  It is invalid to request
   * removal of the mask entry from an ACL that would otherwise require a mask
   * entry, due to existing named entries or an unnamed group entry.
   *
   * @param existingAcl List<AclEntry> existing ACL
   * @param inAclSpec List<AclEntry> ACL spec describing entries to filter
   * @return List<AclEntry> new ACL
   * @throws AclException if validation fails
   */
  public static List<AclEntry> filterAclEntriesByAclSpec(
      List<AclEntry> existingAcl, List<AclEntry> inAclSpec) throws AclException {
    ValidatedAclSpec aclSpec = new ValidatedAclSpec(inAclSpec);
    ArrayList<AclEntry> aclBuilder = Lists.newArrayListWithCapacity(MAX_ENTRIES);
    EnumMap<AclEntryScope, AclEntry> providedMask =
      Maps.newEnumMap(AclEntryScope.class);
    EnumSet<AclEntryScope> maskDirty = EnumSet.noneOf(AclEntryScope.class);
    EnumSet<AclEntryScope> scopeDirty = EnumSet.noneOf(AclEntryScope.class);
    for (AclEntry existingEntry: existingAcl) {
      if (aclSpec.containsKey(existingEntry)) {
        scopeDirty.add(existingEntry.getScope());
        if (existingEntry.getType() == AclEntryType.MASK) {
          maskDirty.add(existingEntry.getScope());
        }
      } else {
        if (existingEntry.getType() == AclEntryType.MASK) {
          providedMask.put(existingEntry.getScope(), existingEntry);
        } else {
          aclBuilder.add(existingEntry);
        }
      }
    }
    copyDefaultsIfNeeded(aclBuilder);
    calculateMasks(aclBuilder, providedMask, maskDirty, scopeDirty);
    return buildAndValidateAcl(aclBuilder);
  }

  /**
   * Filters (discards) any existing default ACL entries.  The new ACL retains
   * only the access ACL entries.
   *
   * @param existingAcl List<AclEntry> existing ACL
   * @return List<AclEntry> new ACL
   * @throws AclException if validation fails
   */
  public static List<AclEntry> filterDefaultAclEntries(
      List<AclEntry> existingAcl) throws AclException {
    ArrayList<AclEntry> aclBuilder = Lists.newArrayListWithCapacity(MAX_ENTRIES);
    for (AclEntry existingEntry: existingAcl) {
      if (existingEntry.getScope() == AclEntryScope.DEFAULT) {
        // Default entries sort after access entries, so we can exit early.
        break;
      }
      aclBuilder.add(existingEntry);
    }
    return buildAcl(aclBuilder);
  }

  /**
   * Merges the entries of the ACL spec into the existing ACL.  If necessary,
   * recalculates the mask entries.  If necessary, default entries may be
   * inferred by copying the permissions of the corresponding access entries.
   *
   * @param existingAcl List<AclEntry> existing ACL
   * @param inAclSpec List<AclEntry> ACL spec containing entries to merge
   * @return List<AclEntry> new ACL
   * @throws AclException if validation fails
   */
  public static List<AclEntry> mergeAclEntries(List<AclEntry> existingAcl,
      List<AclEntry> inAclSpec) throws AclException {
    ValidatedAclSpec aclSpec = new ValidatedAclSpec(inAclSpec);
    ArrayList<AclEntry> aclBuilder = Lists.newArrayListWithCapacity(MAX_ENTRIES);
    List<AclEntry> foundAclSpecEntries =
      Lists.newArrayListWithCapacity(MAX_ENTRIES);
    EnumMap<AclEntryScope, AclEntry> providedMask =
      Maps.newEnumMap(AclEntryScope.class);
    EnumSet<AclEntryScope> maskDirty = EnumSet.noneOf(AclEntryScope.class);
    EnumSet<AclEntryScope> scopeDirty = EnumSet.noneOf(AclEntryScope.class);
    for (AclEntry existingEntry: existingAcl) {
      AclEntry aclSpecEntry = aclSpec.findByKey(existingEntry);
      if (aclSpecEntry != null) {
        foundAclSpecEntries.add(aclSpecEntry);
        scopeDirty.add(aclSpecEntry.getScope());
        if (aclSpecEntry.getType() == AclEntryType.MASK) {
          providedMask.put(aclSpecEntry.getScope(), aclSpecEntry);
          maskDirty.add(aclSpecEntry.getScope());
        } else {
          aclBuilder.add(aclSpecEntry);
        }
      } else {
        if (existingEntry.getType() == AclEntryType.MASK) {
          providedMask.put(existingEntry.getScope(), existingEntry);
        } else {
          aclBuilder.add(existingEntry);
        }
      }
    }
    // ACL spec entries that were not replacements are new additions.
    for (AclEntry newEntry: aclSpec) {
      if (Collections.binarySearch(foundAclSpecEntries, newEntry) < 0) {
        scopeDirty.add(newEntry.getScope());
        if (newEntry.getType() == AclEntryType.MASK) {
          providedMask.put(newEntry.getScope(), newEntry);
          maskDirty.add(newEntry.getScope());
        } else {
          aclBuilder.add(newEntry);
        }
      }
    }
    copyDefaultsIfNeeded(aclBuilder);
    calculateMasks(aclBuilder, providedMask, maskDirty, scopeDirty);
    return buildAndValidateAcl(aclBuilder);
  }

  /**
   * Completely replaces the ACL with the entries of the ACL spec.  If
   * necessary, recalculates the mask entries.  If necessary, default entries
   * are inferred by copying the permissions of the corresponding access
   * entries.  Replacement occurs separately for each of the access ACL and the
   * default ACL.  If the ACL spec contains only access entries, then the
   * existing default entries are retained.  If the ACL spec contains only
   * default entries, then the existing access entries are retained.  If the ACL
   * spec contains both access and default entries, then both are replaced.
   *
   * @param existingAcl List<AclEntry> existing ACL
   * @param inAclSpec List<AclEntry> ACL spec containing replacement entries
   * @return List<AclEntry> new ACL
   * @throws AclException if validation fails
   */
  public static List<AclEntry> replaceAclEntries(List<AclEntry> existingAcl,
      List<AclEntry> inAclSpec) throws AclException {
    ValidatedAclSpec aclSpec = new ValidatedAclSpec(inAclSpec);
    ArrayList<AclEntry> aclBuilder = Lists.newArrayListWithCapacity(MAX_ENTRIES);
    // Replacement is done separately for each scope: access and default.
    EnumMap<AclEntryScope, AclEntry> providedMask =
      Maps.newEnumMap(AclEntryScope.class);
    EnumSet<AclEntryScope> maskDirty = EnumSet.noneOf(AclEntryScope.class);
    EnumSet<AclEntryScope> scopeDirty = EnumSet.noneOf(AclEntryScope.class);
    for (AclEntry aclSpecEntry: aclSpec) {
      scopeDirty.add(aclSpecEntry.getScope());
      if (aclSpecEntry.getType() == AclEntryType.MASK) {
        providedMask.put(aclSpecEntry.getScope(), aclSpecEntry);
        maskDirty.add(aclSpecEntry.getScope());
      } else {
        aclBuilder.add(aclSpecEntry);
      }
    }
    // Copy existing entries if the scope was not replaced.
    for (AclEntry existingEntry: existingAcl) {
      if (!scopeDirty.contains(existingEntry.getScope())) {
        if (existingEntry.getType() == AclEntryType.MASK) {
          providedMask.put(existingEntry.getScope(), existingEntry);
        } else {
          aclBuilder.add(existingEntry);
        }
      }
    }
    copyDefaultsIfNeeded(aclBuilder);
    calculateMasks(aclBuilder, providedMask, maskDirty, scopeDirty);
    return buildAndValidateAcl(aclBuilder);
  }

  /**
   * There is no reason to instantiate this class.
   */
  private AclTransformation() {
  }

  /**
   * Builds the final list of ACL entries to return by sorting and trimming
   * the ACL entries that have been added.
   *
   * @param aclBuilder ArrayList<AclEntry> containing entries to build
   * @return List<AclEntry> unmodifiable, sorted list of ACL entries
   */
  private static List<AclEntry> buildAndValidateAcl(
      ArrayList<AclEntry> aclBuilder) throws AclException {
    System.out.println("cn aclBuilder = " + aclBuilder);
    if (aclBuilder.size() > MAX_ENTRIES) {
      throw new AclException("Invalid ACL: ACL has " + aclBuilder.size() +
        " entries, which exceeds maximum of " + MAX_ENTRIES + ".");
    }
    aclBuilder.trimToSize();
    Collections.sort(aclBuilder);
    AclEntry userEntry = null, groupEntry = null, otherEntry = null;
    AclEntry prevEntry = null;
    for (AclEntry entry: aclBuilder) {
      if (prevEntry != null && prevEntry.compareTo(entry) == 0) {
        throw new AclException(
          "Invalid ACL: multiple entries with same scope, type and name.");
      }
      if (entry.getName() != null && (entry.getType() == AclEntryType.MASK ||
          entry.getType() == AclEntryType.OTHER)) {
        throw new AclException(
          "Invalid ACL: this entry type must not have a name: " + entry + ".");
      }
      if (entry.getScope() == AclEntryScope.ACCESS) {
        if (entry.getType() == AclEntryType.USER && entry.getName() == null) {
          userEntry = entry;
        }
        if (entry.getType() == AclEntryType.GROUP && entry.getName() == null) {
          groupEntry = entry;
        }
        if (entry.getType() == AclEntryType.OTHER && entry.getName() == null) {
          otherEntry = entry;
        }
      }
      prevEntry = entry;
    }
    if (userEntry == null || groupEntry == null || otherEntry == null) {
      throw new AclException(
        "Invalid ACL: the user, group and other entries are required.");
    }
    return Collections.unmodifiableList(aclBuilder);
  }

  private static void calculateMasks(List<AclEntry> aclBuilder,
      EnumMap<AclEntryScope, AclEntry> providedMask,
      EnumSet<AclEntryScope> maskDirty, EnumSet<AclEntryScope> scopeDirty)
      throws AclException {
    EnumSet<AclEntryScope> scopeFound = EnumSet.noneOf(AclEntryScope.class);
    EnumMap<AclEntryScope, FsAction> unionPerms =
      Maps.newEnumMap(AclEntryScope.class);
    EnumSet<AclEntryScope> maskNeeded = EnumSet.noneOf(AclEntryScope.class);
    for (AclEntry entry: aclBuilder) {
      scopeFound.add(entry.getScope());
      if (entry.getType() == AclEntryType.GROUP ||
          entry.getName() != null) {
        FsAction scopeUnionPerms = Objects.firstNonNull(unionPerms.get(entry.getScope()),
          FsAction.NONE);
        unionPerms.put(entry.getScope(), scopeUnionPerms.or(entry.getPermission()));
      }
      if (entry.getName() != null) {
        maskNeeded.add(entry.getScope());
      }
    }
    for (AclEntryScope scope: scopeFound) {
      if (!providedMask.containsKey(scope) && maskNeeded.contains(scope) &&
          maskDirty.contains(scope)) {
        throw new AclException(
          "Invalid ACL: mask is required, but it was deleted.");
      } else if (providedMask.containsKey(scope) && (!scopeDirty.contains(scope) ||
          maskDirty.contains(scope))) {
        aclBuilder.add(providedMask.get(scope));
      } else if (maskNeeded.contains(scope)) {
        aclBuilder.add(new AclEntry.Builder()
          .setScope(scope)
          .setType(AclEntryType.MASK)
          .setPermission(unionPerms.get(scope))
          .build());
      }
    }
  }

  private static void copyDefaultsIfNeeded(List<AclEntry> aclBuilder) {
    AclEntry userEntry = null, groupEntry = null, otherEntry = null;
    AclEntry defaultUserEntry = null, defaultGroupEntry = null,
      defaultOtherEntry = null;
    for (AclEntry entry: aclBuilder) {
      if (entry.getScope() == AclEntryScope.ACCESS) {
        if (entry.getType() == AclEntryType.USER && entry.getName() == null) {
          userEntry = entry;
        }
        if (entry.getType() == AclEntryType.GROUP && entry.getName() == null) {
          groupEntry = entry;
        }
        if (entry.getType() == AclEntryType.OTHER && entry.getName() == null) {
          otherEntry = entry;
        }
      } else {
        if (entry.getType() == AclEntryType.USER && entry.getName() == null) {
          defaultUserEntry = entry;
        }
        if (entry.getType() == AclEntryType.GROUP && entry.getName() == null) {
          defaultGroupEntry = entry;
        }
        if (entry.getType() == AclEntryType.OTHER && entry.getName() == null) {
          defaultOtherEntry = entry;
        }
      }
    }
    if (defaultUserEntry != null || defaultGroupEntry != null ||
        defaultOtherEntry != null) {
      if (defaultUserEntry == null) aclBuilder.add(getDefaultEntryOrCopy(defaultUserEntry, userEntry));
      if (defaultGroupEntry == null) aclBuilder.add(getDefaultEntryOrCopy(defaultGroupEntry, groupEntry));
      if (defaultOtherEntry == null) aclBuilder.add(getDefaultEntryOrCopy(defaultOtherEntry, otherEntry));
    }
  }

  /**
   * Returns the first entry if not null.  Otherwise, creates a copy of the
   * second entry with the scope changed to default and returns that.
   *
   * @param defaultEntry AclEntry provided default entry
   * @param entryToCopy AclEntry entry to copy if the provided entry is null
   * @return AclEntry defaultEntry or copy of entryToCopy with default scope
   */
  private static AclEntry getDefaultEntryOrCopy(AclEntry defaultEntry,
        AclEntry entryToCopy) {
    if (defaultEntry != null) {
      return defaultEntry;
    } else {
      return new AclEntry.Builder()
        .setScope(AclEntryScope.DEFAULT)
        .setType(entryToCopy.getType())
        .setPermission(entryToCopy.getPermission())
        .build();
    }
  }

  /**
   * Adds a new ACL entry to the builder after checking that the result would
   * not exceed the maximum number of entries in a single ACL.
   *
   * @param aclBuilder List<AclEntry> for adding entries
   * @param entry AclEntry entry to add
   * @throws AclException if adding the entry would exceed the maximum number of
   *   entries in a single ACL
   */
  private static void addEntryOrThrow(List<AclEntry> aclBuilder, AclEntry entry)
      throws AclException {
    if (aclBuilder.size() >= MAX_ENTRIES) {
      throw new AclException(
        "Invalid ACL: result exceeds maximum of " + MAX_ENTRIES + " entries.");
    }
    aclBuilder.add(entry);
  }

  /**
   * Builds the final list of ACL entries to return by sorting and trimming
   * the ACL entries that have been added.
   *
   * @param aclBuilder ArrayList<AclEntry> containing entries to build
   * @return List<AclEntry> unmodifiable, sorted list of ACL entries
   */
  private static List<AclEntry> buildAcl(ArrayList<AclEntry> aclBuilder) {
    Collections.sort(aclBuilder);
    aclBuilder.trimToSize();
    return Collections.unmodifiableList(aclBuilder);
  }

  /**
   * An ACL spec that has been pre-validated and sorted.
   */
  private static final class ValidatedAclSpec implements Iterable<AclEntry> {
    private final List<AclEntry> aclSpec;

    /**
     * Creates a ValidatedAclSpec by pre-validating and sorting the given ACL
     * entries.  Pre-validation checks that it does not exceed the maximum
     * entries.  This check is performed before modifying the ACL, and it's
     * actually insufficient for enforcing the maximum number of entries.
     * Transformation logic can create additional entries automatically,such as
     * the mask and some of the default entries, so we also need additional
     * checks during transformation.  The up-front check is still valuable here
     * so that we don't run a lot of expensive transformation logic while
     * holding the namesystem lock for an attacker who intentionally sent a huge
     * ACL spec.
     *
     * @param aclSpec List<AclEntry> containing unvalidated input ACL spec
     * @throws AclException if validation fails
     */
    public ValidatedAclSpec(List<AclEntry> aclSpec) throws AclException {
      if (aclSpec.size() > MAX_ENTRIES) {
        throw new AclException("Invalid ACL: ACL spec has " + aclSpec.size() +
          " entries, which exceeds maximum of " + MAX_ENTRIES + ".");
      }
      Collections.sort(aclSpec);
      this.aclSpec = aclSpec;
    }

    /**
     * Returns true if this contains an entry matching the given key.  An ACL
     * entry's key consists of scope, type and name (but not permission).
     *
     * @param key AclEntry search key
     * @return boolean true if found
     */
    public boolean containsKey(AclEntry key) {
      return Collections.binarySearch(aclSpec, key) >= 0;
    }

    /**
     * Returns the entry matching the given key or null if not found.  An ACL
     * entry's key consists of scope, type and name (but not permission).
     *
     * @param key AclEntry search key
     * @return AclEntry entry matching the given key or null if not found
     */
    public AclEntry findByKey(AclEntry key) {
      int index = Collections.binarySearch(aclSpec, key);
      if (index >= 0) {
        return aclSpec.get(index);
      }
      return null;
    }

    @Override
    public Iterator<AclEntry> iterator() {
      return aclSpec.iterator();
    }
  }
}
