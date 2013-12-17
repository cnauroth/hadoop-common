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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.AclException;

@InterfaceAudience.LimitedPrivate({"HDFS"})
abstract class AclTransformation {
  private static final int MAX_ENTRIES = 32;

  protected final Acl.Builder aclBuilder = new Acl.Builder(MAX_ENTRIES);

  public abstract Acl apply(Acl existingAcl) throws AclException;

  public static AclTransformation filterAclEntriesByAclSpec(
      final List<AclEntry> aclSpec) {
    return new AclTransformation() {
      @Override
      public Acl apply(Acl existingAcl) throws AclException {
        preValidateAclSpec(aclSpec);
        startAclBuilder(existingAcl);
        AclSpecTransformationState state = new AclSpecTransformationState();
        Iterator<AclEntry> aclSpecIter = aclSpec.iterator();
        AclEntry aclSpecEntry = null;
        for (AclEntry existingEntry: existingAcl.getEntries()) {
          aclSpecEntry = advanceIfNull(aclSpecIter, aclSpecEntry);
          if (aclSpecEntry != null) {
            if (existingEntry.compareTo(aclSpecEntry) != 0) {
              state.copyExistingEntry(existingEntry);
            } else {
              state.deleteExistingEntry(existingEntry);
              aclSpecEntry = null;
            }
          } else {
            state.copyExistingEntry(existingEntry);
          }
        }
        state.complete();
        return buildAndValidate(aclBuilder);
      }
    };
  }

  public static AclTransformation filterDefaultAclEntries() {
    return new AclTransformation() {
      @Override
      public Acl apply(Acl existingAcl) throws AclException {
        startAclBuilder(existingAcl);
        for (AclEntry existingEntry: existingAcl.getEntries()) {
          if (existingEntry.getScope() == AclEntryScope.DEFAULT) {
            break;
          }
          aclBuilder.addEntry(existingEntry);
        }
        return buildAndValidate(aclBuilder);
      }
    };
  }

  public static AclTransformation filterExtendedAclEntries() {
    return new AclTransformation() {
      @Override
      public Acl apply(Acl existingAcl) throws AclException {
        startAclBuilder(existingAcl);
        for (AclEntry existingEntry: existingAcl.getEntries()) {
          if (existingEntry.getScope() == AclEntryScope.DEFAULT) {
            break;
          }
          if (existingEntry.getType() != AclEntryType.MASK &&
              existingEntry.getName() == null) {
            aclBuilder.addEntry(existingEntry);
          }
        }
        return buildAndValidate(aclBuilder);
      }
    };
  }

  public static AclTransformation mergeAclEntries(
      final List<AclEntry> aclSpec) {
    return new AclTransformation() {
      @Override
      public Acl apply(Acl existingAcl) throws AclException {
        preValidateAclSpec(aclSpec);
        startAclBuilder(existingAcl);
        AclSpecTransformationState state = new AclSpecTransformationState();
        Iterator<AclEntry> aclSpecIter = aclSpec.iterator();
        AclEntry aclSpecEntry = null;
        for (AclEntry existingEntry: existingAcl.getEntries()) {
          aclSpecEntry = advanceIfNull(aclSpecIter, aclSpecEntry);
          if (aclSpecEntry != null) {
            int comparison = existingEntry.compareTo(aclSpecEntry);
            if (comparison < 0) {
              state.copyExistingEntry(existingEntry);
            } else if (comparison == 0) {
              state.modifyEntry(aclSpecEntry);
              aclSpecEntry = null;
            } else {
              do {
                state.modifyEntry(aclSpecEntry);
                aclSpecEntry = null;
                aclSpecEntry = advanceIfNull(aclSpecIter, aclSpecEntry);
              } while (aclSpecEntry != null &&
                  existingEntry.compareTo(aclSpecEntry) > 0);
              state.copyExistingEntry(existingEntry);
            }
          } else {
            state.copyExistingEntry(existingEntry);
          }
        }
        if (aclSpecEntry != null) {
          state.modifyEntry(aclSpecEntry);
        }
        while (aclSpecIter.hasNext()) {
          state.modifyEntry(aclSpecIter.next());
        }
        state.complete();
        return buildAndValidate(aclBuilder);
      }
    };
  }

  public static AclTransformation replaceAclEntries(
      final List<AclEntry> aclSpec) {
    return new AclTransformation() {
      @Override
      public Acl apply(Acl existingAcl) throws AclException {
        preValidateAclSpec(aclSpec);
        startAclBuilder(existingAcl);
        AclSpecTransformationState state = new AclSpecTransformationState();
        Iterator<AclEntry> existingIter = existingAcl.getEntries().iterator();
        Iterator<AclEntry> aclSpecIter = aclSpec.iterator();
        AclEntry existingEntry = Iterators.getNext(existingIter, null);
        AclEntry aclSpecEntry = Iterators.getNext(aclSpecIter, null);
        for (AclEntryScope scope:
            EnumSet.of(AclEntryScope.ACCESS, AclEntryScope.DEFAULT)) {
          if (aclSpecEntry != null && aclSpecEntry.getScope() == scope) {
            while (aclSpecEntry != null && aclSpecEntry.getScope() == scope) {
              state.modifyEntry(aclSpecEntry);
              aclSpecEntry = Iterators.getNext(aclSpecIter, null);
            }
          } else {
            while (existingEntry != null && existingEntry.getScope() != scope) {
              existingEntry = Iterators.getNext(existingIter, null);
            }
            while (existingEntry != null && existingEntry.getScope() == scope) {
              state.copyExistingEntry(existingEntry);
              existingEntry = Iterators.getNext(existingIter, null);
            }
          }
        }
        state.complete();
        return buildAndValidate(aclBuilder);
      }
    };
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
    if (aclBuilder.getEntryCount() >= MAX_ENTRIES) {
      throw new AclException(
        "Invalid ACL: result exceeds maximum of " + MAX_ENTRIES + " entries.");
    }
    aclBuilder.addEntry(entry);
  }

  protected void startAclBuilder(Acl existingAcl) {
    aclBuilder.setStickyBit(existingAcl.getStickyBit());
  }

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

  private static Acl buildAndValidate(Acl.Builder aclBuilder) {
    Acl acl = aclBuilder.build();
    AclEntry prevEntry = null;
    boolean foundNamedEntry = false;
    boolean foundMaskEntry = false;
    for (AclEntry entry: acl.getEntries()) {
      if (prevEntry != null && prevEntry.compareTo(entry) == 0) {
        // throw
      }
      if (entry.getName() != null) {
        if (entry.getType() == AclEntryType.MASK ||
            entry.getType() == AclEntryType.OTHER) {
          // throw
        }
      }
      if (entry.getScope() == AclEntryScope.ACCESS) {
        if (entry.getName() != null) {
          foundNamedEntry = true;
        }
        if (entry.getType() == AclEntryType.MASK) {
          foundMaskEntry = true;
        }
      }
      if (foundNamedEntry && !foundMaskEntry) {
        // throw
      }
      prevEntry = entry;
    }
    // TODO: user/group/other entries required
    return acl;
  }

  /**
   * Pre-validates an ACL spec by checking that it does not exceed the maximum
   * entries and then sorting it.  This check is performed before modifying the
   * ACL, and it's actually insufficient for enforcing the maximum number of
   * entries.  Transformation logic can create additional entries automatically,
   * such as the mask and some of the default entries, so we also need
   * additional checks during transformation.  The up-front check is still
   * valuable here so that we don't run a lot of expensive transformation logic
   * for an attacker who intentionally sent a huge ACL spec.
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

  protected final class AclSpecTransformationState {
    AclEntry prevEntry;
    AclEntry userEntry, groupEntry, otherEntry;
    AclEntry defaultUserEntry, defaultGroupEntry, defaultOtherEntry;
    final MaskCalculator accessMask = new MaskCalculator(AclEntryScope.ACCESS);
    final MaskCalculator defaultMask = new MaskCalculator(AclEntryScope.DEFAULT);
    boolean hasDefaultEntries;

    void markDirty(AclEntryScope scope) {
      if (scope == AclEntryScope.ACCESS) {
        accessMask.markDirty();
      } else {
        defaultMask.markDirty();
      }
    }

    void copyExistingEntry(AclEntry entry) throws AclException {
      update(entry);
    }

    void deleteExistingEntry(AclEntry entry) {
      markDirty(entry.getScope());
    }

    void modifyEntry(AclEntry entry) throws AclException {
      update(entry);
      AclEntryScope scope = entry.getScope();
      markDirty(scope);
      if (entry.getType() == AclEntryType.MASK) {
        if (scope == AclEntryScope.ACCESS) {
          accessMask.markModified();
        } else {
          defaultMask.markModified();
        }
      }
    }

    void complete() throws AclException {
      if (userEntry == null || groupEntry == null || otherEntry == null) {
        throw new AclException(
          "Invalid ACL: the user, group and other entries are required.");
      }
      accessMask.addMaskIfNeeded(aclBuilder);
      if (hasDefaultEntries) {
        if (defaultUserEntry == null && userEntry != null) {
          defaultUserEntry = new AclEntry.Builder()
            .setScope(AclEntryScope.DEFAULT)
            .setType(AclEntryType.USER)
            .setPermission(userEntry.getPermission())
            .build();
        }
        if (defaultUserEntry != null) {
          addEntryOrThrow(defaultUserEntry);
        }

        if (defaultGroupEntry == null && groupEntry != null) {
          defaultGroupEntry = new AclEntry.Builder()
            .setScope(AclEntryScope.DEFAULT)
            .setType(AclEntryType.GROUP)
            .setPermission(groupEntry.getPermission())
            .build();
        }
        if (defaultGroupEntry != null) {
          addEntryOrThrow(defaultGroupEntry);
          defaultMask.update(defaultGroupEntry);
        }

        if (defaultOtherEntry == null && otherEntry != null) {
          defaultOtherEntry = new AclEntry.Builder()
            .setScope(AclEntryScope.DEFAULT)
            .setType(AclEntryType.OTHER)
            .setPermission(otherEntry.getPermission())
            .build();
        }
        if (defaultOtherEntry != null) {
          addEntryOrThrow(defaultOtherEntry);
        }

        defaultMask.addMaskIfNeeded(aclBuilder);
      }
    }

    private void update(AclEntry entry) throws AclException {
      validateAclEntry(entry);
      if (entry.getScope() == AclEntryScope.ACCESS) {
        if (entry.getType() != AclEntryType.MASK) {
          addEntryOrThrow(entry);
        }
        accessMask.update(entry);
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

  private final class MaskCalculator {
    final AclEntryScope scope;
    AclEntry providedMask = null;
    FsAction unionPerms = FsAction.NONE;
    boolean maskNeeded = false;
    boolean dirty = false;
    boolean modified = false;

    MaskCalculator(AclEntryScope scope) {
      this.scope = scope;
    }

    void update(AclEntry entry) {
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

    void markDirty() {
      dirty = true;
    }

    void markModified() {
      modified = true;
    }

    void addMaskIfNeeded(Acl.Builder aclBuilder) throws AclException {
      if (providedMask != null && (!dirty || modified)) {
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
