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

  public static AclTransformation filterAclEntriesByAclSpec(
      final List<AclEntry> aclSpec) {
    return new AclTransformation() {
      @Override
      public Acl apply(Acl existingAcl) {
        Collections.sort(aclSpec);
        Acl.Builder aclBuilder = startAclBuilder(existingAcl);
        MaskCalculator maskCalculator = new MaskCalculator();
        Iterator<AclEntry> aclSpecIter = aclSpec.iterator();
        AclEntry aclSpecEntry = null;
        for (AclEntry existingEntry: existingAcl.getEntries()) {
          aclSpecEntry = advance(aclSpecIter, aclSpecEntry, existingEntry);
          if (existingEntry.compareTo(aclSpecEntry) != 0) {
            aclBuilder.addEntry(existingEntry);
            maskCalculator.update(existingEntry);
          }
        }
        maskCalculator.addMaskIfNeeded(aclBuilder);
        return buildAndValidate(aclBuilder);
      }
    };
  }

  public static AclTransformation filterDefaultAclEntries() {
    return FILTER_DEFAULT_ACL_ENTRIES;
  }
  private static final AclTransformation FILTER_DEFAULT_ACL_ENTRIES =
    new AclTransformation() {
      @Override
      public Acl apply(Acl existingAcl) {
        Acl.Builder aclBuilder = startAclBuilder(existingAcl);
        for (AclEntry existingEntry: existingAcl.getEntries()) {
          if (existingEntry.getScope() != AclEntryScope.DEFAULT) {
            aclBuilder.addEntry(existingEntry);
          }
        }
        return aclBuilder.build();
      }
    };

  public static AclTransformation filterExtendedAclEntries() {
    return FILTER_EXTENDED_ACL_ENTRIES;
  }
  private static final AclTransformation FILTER_EXTENDED_ACL_ENTRIES =
    new AclTransformation() {
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

  public static AclTransformation mergeAclEntries(
      final List<AclEntry> aclSpec) {
    return new AclTransformation() {
      @Override
      public Acl apply(Acl existingAcl) {
        Collections.sort(aclSpec);
        Acl.Builder aclBuilder = startAclBuilder(existingAcl);
        MaskCalculator maskCalculator = new MaskCalculator();
        Iterator<AclEntry> aclSpecIter = aclSpec.iterator();
        AclEntry aclSpecEntry = null;
        for (AclEntry existingEntry: existingAcl.getEntries()) {
          aclSpecEntry = advance(aclSpecIter, aclSpecEntry, existingEntry);
          if (aclSpecEntry != null) {
            int comparison = existingEntry.compareTo(aclSpecEntry);
            if (comparison < 0) {
              addEntry(existingEntry, aclBuilder, maskCalculator);
            } else if (comparison == 0) {
              addEntry(aclSpecEntry, aclBuilder, maskCalculator);
              aclSpecEntry = null;
            } else {
              addEntry(aclSpecEntry, aclBuilder, maskCalculator);
              addEntry(existingEntry, aclBuilder, maskCalculator);
              aclSpecEntry = null;
            }
          } else {
            addEntry(existingEntry, aclBuilder, maskCalculator);
          }
        }
        maskCalculator.addMaskIfNeeded(aclBuilder);
        return aclBuilder.build();
      }
    };
  }

  public static AclTransformation replaceAclEntries(
      final List<AclEntry> aclSpec) {
    return new AclTransformation() {
      @Override
      public Acl apply(Acl existingAcl) {
        Collections.sort(aclSpec);
        Acl.Builder aclBuilder = startAclBuilder(existingAcl);
        MaskCalculator maskCalculator = new MaskCalculator();
        for (AclEntry newEntry: aclSpec) {
          if (isAccessMask(newEntry)) {
            maskCalculator.provideMask(newEntry);
          } else {
            aclBuilder.addEntry(newEntry);
            maskCalculator.update(newEntry);
          }
        }
        maskCalculator.addMaskIfNeeded(aclBuilder);
        return aclBuilder.build();
      }
    };
  }

  /**
   * There is no reason to instantiate this class.
   */
  private AclTransformation() {
  }

  private static void addEntry(AclEntry entry, Acl.Builder aclBuilder,
      MaskCalculator maskCalculator) {
    if (isAccessMask(entry)) {
      maskCalculator.provideMask(entry);
    } else {
      aclBuilder.addEntry(entry);
      maskCalculator.update(entry);
    }
  }

  private static AclEntry advance(Iterator<AclEntry> aclSpecIter,
      AclEntry aclSpecEntry, AclEntry existingEntry) {
    while (aclSpecIter.hasNext() && (aclSpecEntry == null ||
        aclSpecEntry.compareTo(existingEntry) < 0)) {
      aclSpecEntry = aclSpecIter.next();
    }
    return aclSpecEntry;
  }

  private static Acl buildAndValidate(Acl.Builder aclBuilder) {
    Acl acl = aclBuilder.build();
    AclEntry prevEntry = null;
    boolean foundNamedEntry = false;
    boolean foundMaskEntry = false;
    for (AclEntry entry: acl.getEntries()) {
      if (prevEntry.compareTo(entry) == 0) {
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

  private static boolean isAccessMask(AclEntry entry) {
    return entry.getScope() == AclEntryScope.ACCESS &&
      entry.getType() == AclEntryType.MASK;
  }

  private static Acl.Builder startAclBuilder(Acl existingAcl) {
    return new Acl.Builder().setStickyBit(existingAcl.getStickyBit());
  }

  private static class MaskCalculator {
    private AclEntry providedMask = null;
    private FsAction unionPerms = FsAction.NONE;
    private boolean foundNamedEntries = false;

    public void provideMask(AclEntry providedMask) {
      this.providedMask = providedMask;
    }

    public void update(AclEntry entry) {
      if (providedMask == null) {
        if (entry.getScope() == AclEntryScope.ACCESS) {
          if (entry.getType() == AclEntryType.GROUP ||
              entry.getName() != null) {
            unionPerms = unionPerms.or(entry.getPermission());
          }
          if (entry.getName() != null) {
            foundNamedEntries = true;
          }
        }
      }
    }

    public void addMaskIfNeeded(Acl.Builder aclBuilder) {
      if (providedMask != null) {
        aclBuilder.addEntry(providedMask);
      } else if (foundNamedEntries) {
        aclBuilder.addEntry(new AclEntry.Builder()
          .setScope(AclEntryScope.ACCESS)
          .setType(AclEntryType.MASK)
          .setPermission(unionPerms)
          .build());
      }
    }
  }
}
