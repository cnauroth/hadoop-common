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
package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileContextTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;


/**
 * This class is for  setup and teardown for viewFs so that
 * it can be tested via the standard FileContext tests.
 * 
 * If tests launched via ant (build.xml) the test root is absolute path
 * If tests launched via eclipse, the test root is 
 * is a test dir below the working directory. (see FileContextTestHelper).
 * Since viewFs has no built-in wd, its wd is /user/<username>.
 * 
 * We set up fc to be the viewFs with mount point for 
 * /<firstComponent>" pointing to the local file system's testdir 
 */
public class ViewFsTestSetup extends ViewTestSetupBase {


   /* 
   * return the ViewFS File context to be used for tests
   */
  static public FileContext setupForViewFsLocalFs() throws Exception {
    /**
     * create the test root on local_fs - the  mount table will point here
     */
    FileContext fclocal = FileContext.getLocalFSFileContext();
    Path targetOfTests = FileContextTestHelper.getTestRootPath(fclocal);
    // In case previous test was killed before cleanup
    fclocal.delete(targetOfTests, true);
    
    fclocal.mkdir(targetOfTests, FileContext.DEFAULT_PERM, true);
  
    // Set up the defaultMT in the config with mount point links
    Configuration conf = new Configuration();
    ConfigUtil.addLink(conf, getTestMountPoint(), targetOfTests.toUri());
    
    FileContext fc = FileContext.getFileContext(FsConstants.VIEWFS_URI, conf);
    return fc;
  }

  /**
   * 
   * delete the test directory in the target local fs
   */
  static public void tearDownForViewFsLocalFs() throws Exception {
    FileContext fclocal = FileContext.getLocalFSFileContext();
    Path targetOfTests = FileContextTestHelper.getTestRootPath(fclocal);
    fclocal.delete(targetOfTests, true);
  }

}
