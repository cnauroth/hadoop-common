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

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Acl {
  private final Path file;
  private final String owner;
  private final String group;
  private final Set<AclEntry> entries;

  public Path getFile() {
    return file;
  }

  public String getOwner() {
    return owner;
  }

  public String getGroup() {
    return group;
  }

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
      .append(",owner: ").append(owner)
      .append(",group: ").append(group)
      .append(",entries: ").append(entries)
      .toString();
  }

  public static class Builder {
    private Path file;
    private String owner;
    private String group;
    private Set<AclEntry> entries = new LinkedHashSet<AclEntry>();

    public Builder addEntry(AclEntry entry) {
      entries.add(entry);
      return this;
    }

    public Builder setFile(Path file) {
      this.file = file;
      return this;
    }

    public Builder setOwner(String owner) {
      this.owner = owner;
      return this;
    }

    public Builder setGroup(String group) {
      this.group = group;
      return this;
    }

    public Acl build() {
      return new Acl(file, owner, group, entries);
    }
  }

  private Acl(Path file, String owner, String group, Set<AclEntry> entries) {
    this.file = file;
    this.owner = owner;
    this.group = group;
    this.entries = Collections.unmodifiableSet(
      new LinkedHashSet<AclEntry>(entries));
  }
}
