package org.apache.hadoop.fs.azurenative;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.junit.*;

import com.microsoft.windowsazure.services.blob.client.*;

import org.apache.hadoop.fs.azure.AzureException;
import org.apache.hadoop.fs.azurenative.AzureBlobStorageTestAccount.CreateOptions;

/**
 * Tests that ASV creates containers only if needed.
 */
public class TestContainerChecks {
  private AzureBlobStorageTestAccount testAccount;

  @After
  public void tearDown() throws Exception {
    if (testAccount != null) {
      testAccount.cleanup();
      testAccount = null;
    }
  }

  @Test
  public void testContainerCreateOnWrite() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create("",
            EnumSet.noneOf(CreateOptions.class));
    assumeNotNull(testAccount);
    CloudBlobContainer container = testAccount.getRealContainer();
    FileSystem fs = testAccount.getFileSystem();

    // Starting off with the container not there
    assertFalse(container.exists());

    // A list shouldn't create the container.
    assertNull(fs.listStatus(new Path("/")));
    assertFalse(container.exists());
    
    // Neither should a read.
    try {
      fs.open(new Path("/foo"));
      assertFalse("Should've thrown.", true);
    } catch (FileNotFoundException ex) {
    }
    assertFalse(container.exists());

    // Neither should a rename
    assertFalse(fs.rename(new Path("/foo"), new Path("/bar")));
    assertFalse(container.exists());

    // But a write should.
    assertTrue(fs.createNewFile(new Path("/foo")));
    assertTrue(container.exists());
  }

  @Test
  public void testContainerChecksWithSas() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create("",
        EnumSet.of(CreateOptions.UseSas));
    assumeNotNull(testAccount);
    CloudBlobContainer container = testAccount.getRealContainer();
    FileSystem fs = testAccount.getFileSystem();

    // The container shouldn't be there
    assertFalse(container.exists());

    // A write should just fail
    try {
      fs.createNewFile(new Path("/foo"));
      assertFalse("Should've thrown.", true);
    } catch (AzureException ex) {
    }
    assertFalse(container.exists());
  }
}
