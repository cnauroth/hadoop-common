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
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.PermissionSource.INodePermissionSource;
import org.apache.hadoop.hdfs.server.namenode.PermissionSource.INodeSnapshotPermissionSource;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

@InterfaceAudience.LimitedPrivate({"HDFS"})
public interface PermissionTarget extends PermissionSource {
  void setFsPermissionShort(short permission) throws QuotaExceededException;

  public static class INodePermissionTarget extends INodePermissionSource
      implements PermissionTarget {
    public INodePermissionTarget(INode inode) {
      super(inode);
    }

    @Override
    public void setFsPermissionShort(short permission) {
      inode.setFsPermissionShort(permission);
    }
  }

  public static class INodeSnapshotPermissionTarget
      extends INodeSnapshotPermissionSource implements PermissionTarget {
    private final INodeMap inodeMap;

    public INodeSnapshotPermissionTarget(INode inode, Snapshot snapshot,
        INodeMap inodeMap) {
      super(inode, snapshot);
      this.inodeMap = inodeMap;
    }

    @Override
    public void setFsPermissionShort(short permission)
        throws QuotaExceededException {
      inode.setFsPermissionShort(permission, snapshot, inodeMap);
    }
  }
}
