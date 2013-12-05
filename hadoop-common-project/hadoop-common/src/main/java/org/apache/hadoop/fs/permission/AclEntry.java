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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AclEntry {
  private final AclEntryType type;
  private final String name;
  private final FsAction permission;
  private final AclEntryScope scope;

  public AclEntryType getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public FsAction getPermission() {
    return permission;
  }

  public AclEntryScope getScope() {
    return scope;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    AclEntry other = (AclEntry)o;
    return new EqualsBuilder()
      .append(type, other.type)
      .append(name, other.name)
      .append(permission, other.permission)
      .append(scope, other.scope)
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
      .append(type)
      .append(name)
      .append(permission)
      .append(scope)
      .hashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (scope == AclEntryScope.DEFAULT) {
      sb.append("default:");
    }
    if (type != null) {
      sb.append(type);
    }
    sb.append(':');
    if (name != null) {
      sb.append(name);
    }
    sb.append(':');
    if (permission != null) {
      sb.append(permission);
    }
    return sb.toString();
  }

  public static class Builder {
    private AclEntryType type;
    private String name;
    private FsAction permission;
    private AclEntryScope scope;

    public Builder setType(AclEntryType type) {
      this.type = type;
      return this;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setPermission(FsAction permission) {
      this.permission = permission;
      return this;
    }

    public Builder setScope(AclEntryScope scope) {
      this.scope = scope;
      return this;
    }

    public AclEntry build() {
      return new AclEntry(type, name, permission, scope);
    }
  }

  private AclEntry(AclEntryType type, String name, FsAction permission, AclEntryScope scope) {
    this.type = type;
    this.name = name;
    this.permission = permission;
    this.scope = scope;
  }
}
