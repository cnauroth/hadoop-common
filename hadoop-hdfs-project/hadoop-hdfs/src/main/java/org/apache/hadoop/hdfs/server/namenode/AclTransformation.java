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
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

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
 * The algorithms implemented here operate in a single iteration through the
 * existing ACL entries and ACL spec to perform all validation, modifications
 * and calculation of any inferred entries.  In order for this to work, there is
 * a dependency on receiving the ACL entries in sorted order.  For any existing
 * ACL, it is assumed that the entries are sorted.  This is because all ACL
 * creation and modification is intended to go through these methods, and they
 * all guarantee correct sort order in their outputs due to explicit sorting and
 * reliance on the natural ordering defined in {@link AclEntry#compareTo}.
 * However, an ACL spec is considered untrusted user input, so all operations
 * pre-sort the ACL spec as the first step.
 *
 * {@link #filterAclEntriesByAclSpec}, {@link #mergeAclEntries} and
 * {@link #replaceAclEntries} all have a lot in common in terms of needing to
 * track state for validation, mask calculations and automatic creation of
 * default entries if not provided.  Each of these methods delegates to two
 * internal helper classes to manage the required state tracking:
 * {@link #AclSpecTransformationState} and {@link #MaskCalculator}.  For
 * {@link #filterDefaultAclEntries} and {@link #filterExtendedAclEntries}, this
 * state tracking is not required, so these methods do not make use of the
 * helper classes.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public abstract class AclTransformation {
  private static final int MAX_ENTRIES = 32;

  protected final ArrayList<AclEntry> aclBuilder =
    Lists.newArrayListWithCapacity(MAX_ENTRIES);

  /**
   * Applies the transformation to the given Acl.
   *
   * @param existingAcl List<AclEntry> existing ACL
   * @return List<AclEntry> new ACL
   * @throws AclException if validation fails
   */
  public abstract List<AclEntry> apply(List<AclEntry> existingAcl)
      throws AclException;

  /**
   * Filters (discards) any existing ACL entries that have the same scope, type
   * and name of any entry in the ACL spec.  If necessary, recalculates the mask
   * entries.  If necessary, default entries may be inferred by copying the
   * permissions of the corresponding access entries.  It is invalid to request
   * removal of the mask entry from an ACL that would otherwise require a mask
   * entry, due to existing named entries or an unnamed group entry.
   *
   * @param aclSpec List<AclEntry> ACL spec describing entries to filter
   * @return AclTransformation that applies the requested change
   */
  public static AclTransformation filterAclEntriesByAclSpec(
      final List<AclEntry> aclSpec) {
    return new AclTransformation() {
      @Override
      public List<AclEntry> apply(List<AclEntry> existingAcl) throws AclException {
        preValidateAclSpec(aclSpec);
        AclSpecTransformationState state = new AclSpecTransformationState();
        Iterator<AclEntry> aclSpecIter = aclSpec.iterator();
        AclEntry aclSpecEntry = null;
        for (AclEntry existingEntry: existingAcl) {
          aclSpecEntry = advanceIfNull(aclSpecIter, aclSpecEntry);
          if (aclSpecEntry != null) {
            if (existingEntry.compareTo(aclSpecEntry) != 0) {
              // The existing entry was not requested to be removed in the ACL
              // spec, so copy.
              state.copyExistingEntry(existingEntry);
            } else {
              // The existing entry matches an entry in the ACL spec, so delete.
              state.deleteExistingEntry(existingEntry);
              aclSpecEntry = null;
            }
          } else {
            // We've iterated through all entries in the ACL spec, so keep any
            // existing entries that still remain.
            state.copyExistingEntry(existingEntry);
          }
        }
        // At this point, it's possible that the ACL spec still contains entries
        // that we haven't visited.  That's OK, because there are no existing
        // entries remaining that could be a match.
        state.complete();
        return buildAcl();
      }
    };
  }

  /**
   * Filters (discards) any existing default ACL entries.  The new ACL retains
   * only the access ACL entries.
   *
   * @return AclTransformation that applies the requested change
   */
  public static AclTransformation filterDefaultAclEntries() {
    return new AclTransformation() {
      @Override
      public List<AclEntry> apply(List<AclEntry> existingAcl) throws AclException {
        for (AclEntry existingEntry: existingAcl) {
          if (existingEntry.getScope() == AclEntryScope.DEFAULT) {
            // Default entries sort after access entries, so we can exit early.
            break;
          }
          aclBuilder.add(existingEntry);
        }
        return buildAcl();
      }
    };
  }

  /**
   * Filters (discards) any extended ACL entries.  The new ACL will be a minimal
   * ACL that retains only the 3 base access entries: user, group and other.
   *
   * @return AclTransformation that applies the requested change
   */
  public static AclTransformation filterExtendedAclEntries() {
    return new AclTransformation() {
      @Override
      public List<AclEntry> apply(List<AclEntry> existingAcl) throws AclException {
        for (AclEntry existingEntry: existingAcl) {
          if (existingEntry.getScope() == AclEntryScope.DEFAULT) {
            // Default entries sort after access entries, so we can exit early.
            break;
          }
          if (existingEntry.getType() != AclEntryType.MASK &&
              existingEntry.getName() == null) {
            // This is one of the base access entries, so copy.
            aclBuilder.add(existingEntry);
          }
        }
        return buildAcl();
      }
    };
  }

  /**
   * Merges the entries of the ACL spec into the existing ACL.  If necessary,
   * recalculates the mask entries.  If necessary, default entries may be
   * inferred by copying the permissions of the corresponding access entries.
   *
   * @param aclSpec List<AclEntry> ACL spec containing entries to merge
   * @return AclTransformation that applies the requested change
   */
  public static AclTransformation mergeAclEntries(
      final List<AclEntry> aclSpec) {
    return new AclTransformation() {
      @Override
      public List<AclEntry> apply(List<AclEntry> existingAcl) throws AclException {
        preValidateAclSpec(aclSpec);
        AclSpecTransformationState state = new AclSpecTransformationState();
        Iterator<AclEntry> aclSpecIter = aclSpec.iterator();
        AclEntry aclSpecEntry = null;
        for (AclEntry existingEntry: existingAcl) {
          aclSpecEntry = advanceIfNull(aclSpecIter, aclSpecEntry);
          if (aclSpecEntry != null) {
            int comparison = existingEntry.compareTo(aclSpecEntry);
            if (comparison < 0) {
              // The existing entry is less than the ACL spec entry.  The
              // existing entry has not been modified, so copy.  Do not advance
              // the ACL spec.
              state.copyExistingEntry(existingEntry);
            } else if (comparison == 0) {
              // The existing entry has the same scope, type and name as the ACL
              // spec entry.  The existing entry has been modified.  Advance the
              // ACL spec.
              state.modifyEntry(aclSpecEntry);
              aclSpecEntry = null;
            } else {
              // The existing entry is greater than the ACL spec entry.  The ACL
              // spec entry is a new entry, so create.  There may be multiple
              // new entries, so do a catch-up iteration to add all.
              do {
                state.modifyEntry(aclSpecEntry);
                aclSpecEntry = advanceIfNull(aclSpecIter, null);
              } while (aclSpecEntry != null &&
                  existingEntry.compareTo(aclSpecEntry) > 0);
              // After catching up, the final ACL spec entry might match the
              // existing entry.  Either modify or copy the existing.
              if (aclSpecEntry != null &&
                  existingEntry.compareTo(aclSpecEntry) == 0) {
                state.modifyEntry(aclSpecEntry);
                aclSpecEntry = null;
              } else {
                state.copyExistingEntry(existingEntry);
              }
            }
          } else {
            // We've iterated through all entries in the ACL spec, so keep any
            // existing entries that still remain.
            state.copyExistingEntry(existingEntry);
          }
        }
        // If the ACL spec contains any remaining entries, then they are new
        // entries that need to be created.
        if (aclSpecEntry != null) {
          state.modifyEntry(aclSpecEntry);
        }
        while (aclSpecIter.hasNext()) {
          state.modifyEntry(aclSpecIter.next());
        }
        state.complete();
        return buildAcl();
      }
    };
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
   * @param aclSpec List<AclEntry> ACL spec containing replacement entries
   * @return AclTransformation that applies the requested change
   */
  public static AclTransformation replaceAclEntries(
      final List<AclEntry> aclSpec) {
    return new AclTransformation() {
      @Override
      public List<AclEntry> apply(List<AclEntry> existingAcl) throws AclException {
        preValidateAclSpec(aclSpec);
        AclSpecTransformationState state = new AclSpecTransformationState();
        Iterator<AclEntry> existingIter = existingAcl.iterator();
        Iterator<AclEntry> aclSpecIter = aclSpec.iterator();
        AclEntry existingEntry = Iterators.getNext(existingIter, null);
        AclEntry aclSpecEntry = Iterators.getNext(aclSpecIter, null);
        // Replacement is done separately for each scope: access and default.
        for (AclEntryScope scope:
            EnumSet.of(AclEntryScope.ACCESS, AclEntryScope.DEFAULT)) {
          if (aclSpecEntry != null && aclSpecEntry.getScope() == scope) {
            // The ACL spec contains entries for this scope.  Take all entries
            // in this scope from the ACL spec.  Ignore the existing entries in
            // this scope.
            while (aclSpecEntry != null && aclSpecEntry.getScope() == scope) {
              state.modifyEntry(aclSpecEntry);
              aclSpecEntry = Iterators.getNext(aclSpecIter, null);
            }
          } else {
            // If necessary, advance past existing entries that were ignored
            // while handling the prior scope.
            while (existingEntry != null && existingEntry.getScope() != scope) {
              existingEntry = Iterators.getNext(existingIter, null);
            }
            // The ACL spec does not contain entries for this scope.  Keep the
            // existing entries in this scope.
            while (existingEntry != null && existingEntry.getScope() == scope) {
              state.copyExistingEntry(existingEntry);
              existingEntry = Iterators.getNext(existingIter, null);
            }
          }
        }
        state.complete();
        return buildAcl();
      }
    };
  }

  /**
   * Builds the final list of ACL entries to return by sorting and trimming
   * the ACL entries that have been added.
   *
   * @return List<AclEntry> unmodifiable, sorted list of ACL entries
   */
  protected List<AclEntry> buildAcl() {
    Collections.sort(aclBuilder);
    aclBuilder.trimToSize();
    return Collections.unmodifiableList(aclBuilder);
  }

  /**
   * Adds a new ACL entry to the builder after checking that the result would
   * not exceed the maximum number of entries in a single ACL.
   *
   * @param entry AclEntry entry to add
   * @throws AclException if adding the entry would exceed the maximum number of
   *   entries in a single ACL
   */
  private void addEntryOrThrow(AclEntry entry) throws AclException {
    if (aclBuilder.size() >= MAX_ENTRIES) {
      throw new AclException(
        "Invalid ACL: result exceeds maximum of " + MAX_ENTRIES + " entries.");
    }
    aclBuilder.add(entry);
  }

  /**
   * Accepts an iterator and the current entry from that iterator.  Advances the
   * iterator only if necessary and returns the current entry.  Returns null if
   * there are no entries remaining.
   *
   * @param aclSpecIter Iterator<AclEntry> iterator to advance
   * @param aclSpecEntry AclEntry current entry from the iterator
   * @return AclEntry new current entry, null if no entries remaining
   */
  private static AclEntry advanceIfNull(Iterator<AclEntry> aclSpecIter,
      AclEntry aclSpecEntry) {
    if (aclSpecEntry != null) {
      return aclSpecEntry;
    } else if (aclSpecIter.hasNext()) {
      return aclSpecIter.next();
    } else {
      return null;
    }
  }

  /**
   * Pre-validates an ACL spec by checking that it does not exceed the maximum
   * entries and then sorting it.  This check is performed before modifying the
   * ACL, and it's actually insufficient for enforcing the maximum number of
   * entries.  Transformation logic can create additional entries automatically,
   * such as the mask and some of the default entries, so we also need
   * additional checks during transformation.  The up-front check is still
   * valuable here so that we don't run a lot of expensive transformation logic
   * while holding the namesystem lock for an attacker who intentionally sent a
   * huge ACL spec.
   *
   * @param aclSpec List<AclEntry> to validate
   * @throws AclException if validation fails
   */
  private static void preValidateAclSpec(List<AclEntry> aclSpec)
      throws AclException {
    if (aclSpec.size() > MAX_ENTRIES) {
      throw new AclException("Invalid ACL: ACL spec has " + aclSpec.size() +
        " entries, which exceeds maximum of " + MAX_ENTRIES + ".");
    }
    Collections.sort(aclSpec);
  }

  /**
   * Internal helper class for managing common state tracking required for all
   * operations that use an ACL spec.  This class is responsible for:
   * 1. Validating that the operation will produce a valid ACL.
   * 2. Inferring unspecified default entries by copying permissions from the
   *   corresponding access entries.
   * 3. Coordinating with MaskCalculator for mask calculations.
   */
  protected final class AclSpecTransformationState {
    private final MaskCalculator accessMask =
      new MaskCalculator(AclEntryScope.ACCESS);
    private final MaskCalculator defaultMask =
      new MaskCalculator(AclEntryScope.DEFAULT);
    private AclEntry prevEntry;
    private AclEntry userEntry, groupEntry, otherEntry;
    private AclEntry defaultUserEntry, defaultGroupEntry, defaultOtherEntry;
    private boolean hasDefaultEntries;

    /**
     * Indicates that an existing entry is being copied.
     *
     * @param entry AclEntry entry to copy
     * @throws AclException if validation fails
     */
    public void copyExistingEntry(AclEntry entry) throws AclException {
      update(entry);
    }

    /**
     * Indicates that an existing entry is being deleted.
     *
     * @param entry AclEntry entry to delete
     */
    public void deleteExistingEntry(AclEntry entry) {
      MaskCalculator mask = getMaskCalculatorForScope(entry.getScope());
      mask.markScopeDirty();
      if (entry.getType() == AclEntryType.MASK) {
        mask.markMaskDirty();
      }
    }

    /**
     * Indicates that an entry is being modified.  This could be a new entry or
     * modification of an existing entry.
     *
     * @param entry AclEntry entry to modify
     * @throws AclException if validation fails
     */
    public void modifyEntry(AclEntry entry) throws AclException {
      update(entry);
      MaskCalculator mask = getMaskCalculatorForScope(entry.getScope());
      mask.markScopeDirty();
      if (entry.getType() == AclEntryType.MASK) {
        mask.markMaskDirty();
      }
    }

    /**
     * Indicates that all modifications have been completed.  Performs final
     * validation and if necessary adds remaining entries automatically
     * calculated for mask or default ACL.
     *
     * @throws AclException if validation fails
     */
    public void complete() throws AclException {
      if (userEntry == null || groupEntry == null || otherEntry == null) {
        throw new AclException(
          "Invalid ACL: the user, group and other entries are required.");
      }
      accessMask.addMaskIfNeeded();
      if (hasDefaultEntries) {
        addEntryOrThrow(getDefaultEntryOrCopy(defaultUserEntry, userEntry));
        AclEntry completedDefaultGroupEntry = getDefaultEntryOrCopy(
          defaultGroupEntry, groupEntry);
        addEntryOrThrow(completedDefaultGroupEntry);
        defaultMask.update(completedDefaultGroupEntry);
        addEntryOrThrow(getDefaultEntryOrCopy(defaultOtherEntry, otherEntry));
        defaultMask.addMaskIfNeeded();
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
    private AclEntry getDefaultEntryOrCopy(AclEntry defaultEntry,
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
     * Returns the MaskCalculator matching the specified scope.
     *
     * @param scope AclEntryScope requested mask scope
     * @return MaskCalculator matching the specified scope
     */
    private MaskCalculator getMaskCalculatorForScope(AclEntryScope scope) {
      return scope == AclEntryScope.ACCESS ? accessMask : defaultMask;
    }

    /**
     * Common state update method called internally by all ACL entry
     * modifications.  Performs validation of the ACL entry, adds entries to the
     * Acl.Builder, and tracks required state.
     *
     * @param entry AclEntry entry to update
     * @throws AclException if validation fails
     */
    private void update(AclEntry entry) throws AclException {
      validateAclEntry(entry);
      if (entry.getScope() == AclEntryScope.ACCESS) {
        // Don't add the mask entry right away.  Delegate to the MaskCalculator.
        if (entry.getType() != AclEntryType.MASK) {
          addEntryOrThrow(entry);
        }
        accessMask.update(entry);
        // Remember the base user, group and other entries.  If default entries
        // are required, but some of the default base entries are missing, then
        // we will infer the default base entries by copying permissions from
        // the corresponding access entries.
        if (entry.getName() == null) {
          switch (entry.getType()) {
          case USER:
            userEntry = entry;
            break;
          case GROUP:
            groupEntry = entry;
            break;
          case OTHER:
            otherEntry = entry;
            break;
          }
        }
      } else {
        hasDefaultEntries = true;
        // Don't add the mask entry right away.  Delegate to the MaskCalculator.
        // Don't add the base default entries right away.  Some of them may need
        // to be inferred later.
        if (entry.getType() != AclEntryType.MASK) {
          if (entry.getName() == null) {
            switch (entry.getType()) {
            case USER:
              defaultUserEntry = entry;
              break;
            case GROUP:
              defaultGroupEntry = entry;
              break;
            case OTHER:
              defaultOtherEntry = entry;
              break;
            }
          } else {
            addEntryOrThrow(entry);
            defaultMask.update(entry);
          }
        } else {
          defaultMask.update(entry);
        }
      }
    }

    /**
     * Validates the given AclEntry.  This method enforces that:
     * 1. The entry's scope + type + name must be unique.
     * 2. Mask entries and other entries must not have a name.
     *
     * @param AclEntry entry to validate
     * @throws AclException if validation fails
     */
    private void validateAclEntry(AclEntry entry) throws AclException {
      if (prevEntry != null && prevEntry.compareTo(entry) == 0) {
        throw new AclException(
          "Invalid ACL: multiple entries with same scope, type and name.");
      }
      if (entry.getName() != null && (entry.getType() == AclEntryType.MASK ||
          entry.getType() == AclEntryType.OTHER)) {
        throw new AclException(
          "Invalid ACL: this entry type must not have a name: " + entry + ".");
      }
      prevEntry = entry;
    }
  }

  /**
   * Internal helper class for managing calculation of mask entries.  Mask
   * calculation is performed separately for each scope: access and default.
   * This class is responsible for handling the following cases of mask
   * calculation:
   * 1. Throws an exception if the caller attempts to remove the mask entry of
   *   an existing ACL that requires it.  If the ACL has any named entries, then
   *   a mask entry is required.
   * 2. If the caller supplied a mask in the ACL spec, use it.
   * 3. If the caller did not supply a mask, but there are ACL entry changes in
   *   this scope, then automatically calculate a new mask.  The permissions of
   *   the new mask are the union of the permissions on the group entry and all
   *   named entries.
   */
  private final class MaskCalculator {
    private final AclEntryScope scope;
    private AclEntry providedMask = null;
    private FsAction unionPerms = FsAction.NONE;
    private boolean maskNeeded = false;
    private boolean maskDirty = false;
    private boolean scopeDirty = false;

    /**
     * Creates a MaskCalculator in the given scope.
     *
     * @param scope AclEntryScope scope of mask calculation
     */
    public MaskCalculator(AclEntryScope scope) {
      this.scope = scope;
    }

    /**
     * Updates mask calculation state for the given entry.
     *
     * @param entry AclEntry entry to update
     */
    public void update(AclEntry entry) {
      if (providedMask == null) {
        if (entry.getType() == AclEntryType.MASK) {
          providedMask = entry;
        } else {
          if (entry.getType() == AclEntryType.GROUP ||
              entry.getName() != null) {
            unionPerms = unionPerms.or(entry.getPermission());
          }
          if (entry.getName() != null) {
            maskNeeded = true;
          }
        }
      }
    }

    /**
     * Marks that this mask is dirty.  This means that the ACL spec either
     * modified or deleted the mask entry.
     */
    public void markMaskDirty() {
      maskDirty = true;
    }

    /**
     * Marks that this scope is dirty.  This means that the ACL spec made some
     * kind of modification to an ACL entry in this scope: creating a new entry,
     * modifying an existing entry or deleting an existing entry.
     */
    public void markScopeDirty() {
      scopeDirty = true;
    }

    /**
     * Called at the end of mask calculation to validate the mask state and add
     * the mask entry only if it is required.
     *
     * @throws AclException if validation fails
     */
    public void addMaskIfNeeded() throws AclException {
      if (providedMask == null && maskNeeded && maskDirty) {
        throw new AclException(
          "Invalid ACL: mask is required, but it was deleted.");
      } else if (providedMask != null && (!scopeDirty || maskDirty)) {
        addEntryOrThrow(providedMask);
      } else if (maskNeeded) {
        addEntryOrThrow(new AclEntry.Builder()
          .setScope(scope)
          .setType(AclEntryType.MASK)
          .setPermission(unionPerms)
          .build());
      }
    }
  }
}
