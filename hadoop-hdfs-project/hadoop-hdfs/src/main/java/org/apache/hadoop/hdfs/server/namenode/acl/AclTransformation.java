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
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Objects;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;

@InterfaceAudience.LimitedPrivate({"HDFS"})
abstract class AclTransformation implements Function<Acl, Acl> {
  protected final Acl.Builder aclBuilder = new Acl.Builder();

  public static AclTransformation filterAclEntriesByAclSpec(
      final List<AclEntry> aclSpec) {
    return new AclTransformation() {
      @Override
      public Acl apply(Acl existingAcl) {
        Collections.sort(aclSpec);
        startAclBuilder(existingAcl);
        TransformationState state = new TransformationState();
        Iterator<AclEntry> aclSpecIter = aclSpec.iterator();
        AclEntry aclSpecEntry = null;
        for (AclEntry existingEntry: existingAcl.getEntries()) {
          aclSpecEntry = advance(aclSpecIter, aclSpecEntry, existingEntry);
          if (aclSpecEntry != null) {
            if (existingEntry.compareTo(aclSpecEntry) != 0) {
              state.update(existingEntry);
            } else {
              state.markDirty(existingEntry.getScope());
              aclSpecEntry = null;
            }
          } else {
            state.update(existingEntry);
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
      public Acl apply(Acl existingAcl) {
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
      public Acl apply(Acl existingAcl) {
        startAclBuilder(existingAcl);
        for (AclEntry existingEntry: existingAcl.getEntries()) {
          if (existingEntry.getScope() == AclEntryScope.ACCESS &&
              existingEntry.getType() != AclEntryType.MASK &&
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
      public Acl apply(Acl existingAcl) {
        Collections.sort(aclSpec);
        startAclBuilder(existingAcl);
        TransformationState state = new TransformationState();
        Iterator<AclEntry> aclSpecIter = aclSpec.iterator();
        AclEntry aclSpecEntry = null;
        for (AclEntry existingEntry: existingAcl.getEntries()) {
          aclSpecEntry = advance(aclSpecIter, aclSpecEntry, existingEntry);
          if (aclSpecEntry != null) {
            int comparison = existingEntry.compareTo(aclSpecEntry);
            if (comparison < 0) {
              state.update(existingEntry);
            } else if (comparison == 0) {
              state.update(aclSpecEntry);
              state.markDirty(aclSpecEntry.getScope());
              aclSpecEntry = null;
            } else {
              state.update(aclSpecEntry);
              state.markDirty(aclSpecEntry.getScope());
              state.update(existingEntry);
              aclSpecEntry = null;
            }
          } else {
            state.update(existingEntry);
          }
        }
        if (aclSpecEntry != null) {
          state.update(aclSpecEntry);
          state.markDirty(aclSpecEntry.getScope());
        }
        while (aclSpecIter.hasNext()) {
          aclSpecEntry = aclSpecIter.next();
          state.update(aclSpecEntry);
          state.markDirty(aclSpecEntry.getScope());
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
      public Acl apply(Acl existingAcl) {
        Collections.sort(aclSpec);
        startAclBuilder(existingAcl);
        TransformationState state = new TransformationState();
        for (AclEntry newEntry: aclSpec) {
          state.update(newEntry);
        }
        state.complete();
        return buildAndValidate(aclBuilder);
      }
    };
  }

  protected void startAclBuilder(Acl existingAcl) {
    aclBuilder.setStickyBit(existingAcl.getStickyBit());
  }

  private static AclEntry advance(Iterator<AclEntry> aclSpecIter,
      AclEntry aclSpecEntry, AclEntry existingEntry) {
    if (aclSpecEntry == null) {
      while (aclSpecIter.hasNext() && (aclSpecEntry == null ||
          aclSpecEntry.compareTo(existingEntry) < 0)) {
        aclSpecEntry = aclSpecIter.next();
      }
    }
    return aclSpecEntry;
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

  protected final class TransformationState {
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

    void update(AclEntry entry) {
      if (entry.getScope() == AclEntryScope.ACCESS) {
        if (entry.getType() != AclEntryType.MASK) {
          aclBuilder.addEntry(entry);
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
            aclBuilder.addEntry(entry);
            defaultMask.update(entry);
          }
        } else {
          defaultMask.update(entry);
        }
      }
    }

    void complete() {
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
          aclBuilder.addEntry(defaultUserEntry);
        }

        if (defaultGroupEntry == null && groupEntry != null) {
          defaultGroupEntry = new AclEntry.Builder()
            .setScope(AclEntryScope.DEFAULT)
            .setType(AclEntryType.GROUP)
            .setPermission(groupEntry.getPermission())
            .build();
        }
        if (defaultGroupEntry != null) {
          aclBuilder.addEntry(defaultGroupEntry);
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
          aclBuilder.addEntry(defaultOtherEntry);
        }

        defaultMask.addMaskIfNeeded(aclBuilder);
      }
    }
  }

  private static final class MaskCalculator {
    final AclEntryScope scope;
    AclEntry providedMask = null;
    FsAction unionPerms = FsAction.NONE;
    boolean maskNeeded = false;
    boolean dirty = false;

    MaskCalculator(AclEntryScope scope) {
      this.scope = scope;
    }

    void markDirty() {
      dirty = true;
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

    void addMaskIfNeeded(Acl.Builder aclBuilder) {
      if (providedMask != null && !dirty) {
        aclBuilder.addEntry(providedMask);
      } else if (maskNeeded) {
        aclBuilder.addEntry(new AclEntry.Builder()
          .setScope(scope)
          .setType(AclEntryType.MASK)
          .setPermission(unionPerms)
          .build());
      }
    }
  }
}
