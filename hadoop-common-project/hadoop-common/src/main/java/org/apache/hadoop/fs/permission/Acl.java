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
import org.apache.hadoop.fs.Path;

/**
 * Defines an Access Control List, which is a set of rules for enforcement of
 * permissions on a file or directory.  An Acl contains a set of multiple
 * {@link AclEntry} instances.  The ACL entries define the permissions enforced
 * for different classes of users: owner, named user, owning group, named group
 * and others.  The Acl also contains the associated file as a {@link Path}, the
 * file owner and the file group.  Acl instances are immutable.  Use a
 * {@link Builder} to create a new instance.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Acl {
  private final Path file;
  private final String owner;
  private final String group;
  private final Set<AclEntry> entries;

  /**
   * Returns the file associated to this ACL.
   * 
   * @return Path file associated to this ACL
   */
  public Path getFile() {
    return file;
  }

  /**
   * Returns the file owner.
   * 
   * @return String file owner
   */
  public String getOwner() {
    return owner;
  }

  /**
   * Returns the file group.
   * 
   * @return String file group
   */
  public String getGroup() {
    return group;
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
      .append(file, other.file)
      .append(owner, other.owner)
      .append(group, other.group)
      .append(entries, other.entries)
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
      .append(file)
      .append(owner)
      .append(group)
      .append(entries)
      .hashCode();
  }

  @Override
  public String toString() {
    return new StringBuilder()
      .append("file: ").append(file)
      .append(", owner: ").append(owner)
      .append(", group: ").append(group)
      .append(", entries: ").append(entries)
      .toString();
  }

  /**
   * Builder for creating new Acl instances.
   */
  public static class Builder {
    private Path file;
    private String owner;
    private String group;
    private Set<AclEntry> entries = new LinkedHashSet<AclEntry>();

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
     * Sets the file associated to this ACL.
     * 
     * @param file Path file associated to this ACL
     * @return Builder this builder, for call chaining
     */
    public Builder setFile(Path file) {
      this.file = file;
      return this;
    }

    /**
     * Sets the file owner.
     * 
     * @param owner String file owner
     * @return Builder this builder, for call chaining
     */
    public Builder setOwner(String owner) {
      this.owner = owner;
      return this;
    }

    /**
     * Sets the file group.
     * 
     * @param group String file group
     * @return Builder this builder, for call chaining
     */
    public Builder setGroup(String group) {
      this.group = group;
      return this;
    }

    /**
     * Builds a new Acl populated with the set properties.
     * 
     * @return Acl new Acl
     */
    public Acl build() {
      return new Acl(file, owner, group, entries);
    }
  }

  /**
   * Private constructor.
   * 
   * @param file Path file associated to this ACL
   * @param owner String file owner
   * @param group String file group
   * @param entries Set<AclEntry> set of all ACL entries
   */
  private Acl(Path file, String owner, String group, Set<AclEntry> entries) {
    this.file = file;
    this.owner = owner;
    this.group = group;
    this.entries = Collections.unmodifiableSet(
      new LinkedHashSet<AclEntry>(entries));
  }
}
