package org.apache.hadoop.fs.azurenative;

import java.net.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.azure.AzureException;

import junit.framework.*;

public class TestAzureFileSystemErrorConditions extends TestCase {
  public void testNoInitialize() throws Exception {
    AzureNativeFileSystemStore store = new AzureNativeFileSystemStore();
    boolean passed = false;
    try {
      store.retrieveMetadata("foo");
      passed = true;
    } catch (AssertionError e) {
    }
    assertFalse(
        "Doing an operation on the store should throw if not initalized.",
        passed);
  }

  /**
   * Try accessing an unauthorized or non-existent (treated the same)
   * container from ASV.
   */
  public void testAccessUnauthorizedPublicContainer() throws Exception {
    Path noAccessPath = new Path(
        "asv://nonExistentContainer@hopefullyNonExistentAccount/someFile");
    NativeAzureFileSystem.suppressRetryPolicy();
    try {
      FileSystem.get(noAccessPath.toUri(), new Configuration())
        .open(noAccessPath);
      assertTrue("Should've thrown.", false);
    } catch (AzureException ex) {
      assertTrue("Unexpected message in exception " + ex,
          ex.getMessage().contains(
          "Unable to access container nonExistentContainer in account" +
          " hopefullyNonExistentAccount"));
    } finally {
      NativeAzureFileSystem.resumeRetryPolicy();
    }
  }

  private void testBadUriScenario(String badURI) throws Exception {
    try {
      FileSystem.get(new URI(badURI), new Configuration());
      assertTrue("Should've thrown.", false);
    } catch (AzureException ex) {
      URISyntaxException cause = (URISyntaxException)ex.getCause();
      assertNotNull(cause);
      assertTrue("Bad message: " + cause.getMessage(),
          cause.getMessage().contains(badURI));
    } catch (IllegalArgumentException ex) {
      // We also throw that sometimes.
      assertTrue("Bad message: " + ex.getMessage(),
          ex.getMessage().contains("authority"));
    }
  }

  /**
   * Tests various bad URI scenario.
   */
  public void testBadUri() throws Exception {
    testBadUriScenario("asv://two@ats@bad");
    testBadUriScenario("asv:///noAuthority"); // And no default ASV file system
  }
}
