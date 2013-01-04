package org.apache.hadoop.fs.azurenative;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.AzureException;


import junit.framework.TestCase;

public class TestASVUriAndConfiguration extends TestCase {

  private static final int FILE_SIZE = 4096;
  private static final String PATH_DELIMITER = "/";

  protected String accountName;
  protected String accountKey;
  protected static Configuration conf = null;

  private AzureBlobStorageTestAccount testAccount;


  @Override
  protected void tearDown() throws Exception {
    if (testAccount != null) {
      testAccount.cleanup();
      testAccount = null;
    }
  }

  private boolean validateIOStreams (Path filePath) throws IOException {

    // Capture the file system from the test account.
    //
    FileSystem fs = testAccount.getFileSystem();

    // Create and write a file
    //
    OutputStream outputStream = fs.create(filePath);
    outputStream.write(new byte[FILE_SIZE]);
    outputStream.close();

    // Return true if the the count is equivalent to the file size.
    //
    return (FILE_SIZE == readInputStream (filePath));
  }

  private int readInputStream (Path filePath) throws IOException {

    // Capture the file system from the test account.
    //
    FileSystem fs = testAccount.getFileSystem();

    // Read the file
    //
    InputStream inputStream = fs.open(filePath);
    int count = 0;
    while (inputStream.read() >= 0) {
      count++;
    }
    inputStream.close();

    // Return true if the the count is equivalent to the file size.
    //
    return count;
  }


  // Positive tests to exercise making a connection with to Azure account using
  // account key.
  //
  public void testConnectUsingKey() throws Exception {

    testAccount = AzureBlobStorageTestAccount.create();

    // Validate input and output on the connection.
    //
    assertTrue(validateIOStreams(new Path("/asv_scheme")));
  }

  public void testConnectUsingAnonymous() throws Exception {

    // Create test account with anonymous credentials
    //
    testAccount = AzureBlobStorageTestAccount.createAnonymous("testAsv.txt",  FILE_SIZE);

    // Read the file from the public folder using anonymous credentials.
    //
    assertEquals(FILE_SIZE, readInputStream(new Path ("/testAsv.txt")));
  }

  public void testConnectToRoot() throws Exception {

    // Set up blob names.
    //
    final String blobPrefix = 
        String.format ("asvtests-%s-%tQ-blob", System.getProperty("user.name"), new Date());
    final String inblobName = blobPrefix + "_In" + ".txt";
    final String outblobName = blobPrefix + "_Out" + ".txt";

    // Create test account with default root access.
    //
    testAccount = AzureBlobStorageTestAccount.createRoot(inblobName, FILE_SIZE);


    // Read the file from the default container.
    //
    assertEquals(FILE_SIZE, readInputStream(new Path (PATH_DELIMITER + inblobName)));

    try {
      // Capture file system.
      //
      FileSystem fs = testAccount.getFileSystem();

      // Create output path and open an output stream to the root folder.
      //
      Path outputPath = new Path (PATH_DELIMITER + outblobName);
      OutputStream outputStream = fs.create(outputPath);
      fail("Expected an AzureException when writing to root folder.");
      outputStream.write(new byte[FILE_SIZE]);
      outputStream.close();
    } catch (AzureException e) {
      assertTrue (true);
    } catch (Exception e) {
      String errMsg =
          String.format ("Expected AzureException but got %s instead.", e);
      assertTrue(errMsg, false);
    }
  }
}
