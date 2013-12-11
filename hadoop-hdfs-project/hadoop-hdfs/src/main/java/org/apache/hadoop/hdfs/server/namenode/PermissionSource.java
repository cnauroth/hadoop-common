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
public interface PermissionSource {
  short getFsPermissionShort();
  INode getINode();

  public static class INodePermissionSource implements PermissionSource {
    protected final INode inode;

    public INodePermissionSource(INode inode) {
      this.inode = inode;
    }

    @Override
    public short getFsPermissionShort() {
      return inode.getFsPermissionShort();
    }

    @Override
    public INode getINode() {
      return inode;
    }
  }

  public static class INodeSnapshotPermissionSource
      extends INodePermissionSource {
    protected final Snapshot snapshot;

    public INodeSnapshotPermissionSource(INode inode, Snapshot snapshot) {
      super(inode);
      this.snapshot = snapshot;
    }

    @Override
    public short getFsPermissionShort() {
      return inode.getFsPermissionShort(snapshot);
    }
  }
}
