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
package org.apache.hadoop.hdfs.web.resources;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.util.StringUtils;

/** Owner parameter. */
public class AclPermissionParam extends StringParam {
  /** Parameter name. */
  public static final String NAME = "aclspec";
  /** Default parameter value. */
  public static final String DEFAULT = "";

  private static Domain DOMAIN = new Domain(NAME,
      Pattern.compile(DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT));

  /**
   * Constructor.
   * 
   * @param str a string representation of the parameter value.
   */
  public AclPermissionParam(final String str) {
    super(DOMAIN, str == null || str.equals(DEFAULT) ? null : str);
  }

  public AclPermissionParam(List<AclEntry> acl) {
    super(DOMAIN, parseAclSpec(acl) == null
        || parseAclSpec(acl).equals(DEFAULT) ? null : parseAclSpec(acl));
  }

  @Override
  public String getName() {
    return NAME;
  }

  public List<AclEntry> getAclPermission() {
    final String v = getValue();
    return (v != null ? parseAclSpec(v) : parseAclSpec(DEFAULT));
  }

  /**
   * Parse the aclSpec and returns the list of AclEntry objects.
   * 
   * @param aclSpec
   * @return
   */
  private List<AclEntry> parseAclSpec(String aclSpec) {
    List<AclEntry> aclEntries = new ArrayList<AclEntry>();
    Collection<String> aclStrings =
        StringUtils.getStringCollection(aclSpec, ",");
    for (String aclStr : aclStrings) {
      AclEntry.Builder builder = new AclEntry.Builder();
      // Here "::" represent one empty string.
      // StringUtils.getStringCollection() will ignore this.
      String[] split = aclSpec.split(":");
      if (split.length != 3 && !(split.length == 4 && DEFAULT.equals(split[0]))) {
        throw new HadoopIllegalArgumentException("Invalid <aclSpec> : "
            + aclStr);
      }
      int index = 0;
      if (split.length == 4) {
        assert DEFAULT.equals(split[0]);
        // default entry
        index++;
        builder.setScope(AclEntryScope.DEFAULT);
      }
      String type = split[index++];
      AclEntryType aclType = null;
      try {
        aclType = Enum.valueOf(AclEntryType.class, type.toUpperCase());
        builder.setType(aclType);
      } catch (IllegalArgumentException iae) {
        throw new HadoopIllegalArgumentException(
            "Invalid type of acl in <aclSpec> :" + aclStr);
      }

      builder.setName(split[index++]);

      String permission = split[index++];
      FsAction fsAction = FsAction.getFsAction(permission);
      if (null == fsAction) {
        throw new HadoopIllegalArgumentException(
            "Invalid permission in <aclSpec> : " + aclStr);
      }
      builder.setPermission(fsAction);
      aclEntries.add(builder.build());
    }
    return aclEntries;
  }

  /**
   * Parse the list of AclEntry and returns aclspce.
   * 
   * @param List <AclEntry>
   * @return String
   */
  private static String parseAclSpec(List<AclEntry> aclEntry) {
    StringBuilder aclspce = new StringBuilder();
    for (AclEntry acl : aclEntry) {
      if (aclspce.length() != 0)
        aclspce.append(",");
      aclspce.append(acl.getType() + ":");
      aclspce.append(acl.getName() + ":");
      aclspce.append(acl.getPermission().SYMBOL);
    }
    return aclspce.toString();
  }
}