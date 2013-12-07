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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Defines an Access Control List, which is a set of rules for enforcement of
 * permissions on a file or directory.  An Acl contains a set of multiple
 * {@link AclEntry} instances.  The ACL entries define the permissions enforced
 * for different classes of users: owner, named user, owning group, named group
 * and others.  The Acl also contains additional flags associatd with the file,
 * such as the sticky bit.  Acl instances are immutable.  Use a {@link Builder}
 * to create a new instance.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Acl {
  private final Set<AclEntry> entries;
  private final boolean stickyBit;

  /**
   * Returns the sticky bit.
   * 
   * @return boolean sticky bit
   */
  public boolean getStickyBit() {
    return stickyBit;
  }

  /**
   * Returns the set of all ACL entries.  The set is unmodifiable.
   * 
   * @return Set<AclEntry> unmodifiable set of all ACL entries
   */
  public Set<AclEntry> getEntries() {
    return entries;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    Acl other = (Acl)o;
    return new EqualsBuilder()
      .append(entries, other.entries)
      .append(stickyBit, other.stickyBit)
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
      .append(entries)
      .append(stickyBit)
      .hashCode();
  }

  @Override
  public String toString() {
    return new StringBuilder()
      .append("entries: ").append(entries)
      .append(", stickyBit: ").append(stickyBit)
      .toString();
  }

  /**
   * Builder for creating new Acl instances.
   */
  public static class Builder {
    private Set<AclEntry> entries = new LinkedHashSet<AclEntry>();
    private boolean stickyBit = false;

    /**
     * Adds an ACL entry.
     * 
     * @param entry AclEntry entry to add
     * @return Builder this builder, for call chaining
     */
    public Builder addEntry(AclEntry entry) {
      entries.add(entry);
      return this;
    }

    /**
     * Sets sticky bit.  If this method is not called, then the builder assumes
     * false.
     * 
     * @param boolean sticky bit
     * @return Builder this builder, for call chaining
     */
    public Builder setStickyBit(boolean stickyBit) {
      this.stickyBit = stickyBit;
      return this;
    }

    /**
     * Builds a new Acl populated with the set properties.
     * 
     * @return Acl new Acl
     */
    public Acl build() {
      return new Acl(entries, stickyBit);
    }
  }

  /**
   * Private constructor.
   * 
   * @param entries Set<AclEntry> set of all ACL entries
   * @param boolean sticky bit
   */
  private Acl(Set<AclEntry> entries, boolean stickyBit) {
    this.entries = Collections.unmodifiableSet(
      new LinkedHashSet<AclEntry>(entries));
    this.stickyBit = stickyBit;
  }
}
