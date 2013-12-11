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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

@InterfaceAudience.LimitedPrivate({"HDFS"})
public abstract class PermissionTarget {
  protected INode inode;

  public abstract void setFsPermissionShort(short permission);

  public static class INodePermissionTarget extends PermissionTarget {
    public INodePermissionTarget(INode inode) {
      this.inode = inode;
    }

    @Override
    public void setFsPermissionShort(short permission) {
      inode.setFsPermissionShort(permission);
    }
  }

  public static class INodeSnapshotPermissionTarget extends PermissionTarget {
    private Snapshot snapshot;
    private INodeMap inodeMap;

    public INodeSnapshotPermissionTarget(INode inode, Snapshot snapshot,
        INodeMap inodeMap) {
      this.inode = inode;
      this.snapshot = snapshot;
      this.inodeMap = inodeMap;
    }

    @Override
    public void setFsPermissionShort(short permission) {
      inode.setFsPermissionShort(permission);
    }
  }
}
