package org.apache.hadoop.fs.azurenative;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.azure.AzureException;
import org.apache.hadoop.fs.azure.KeyProviderException;
import org.junit.*;

import org.apache.hadoop.fs.azurenative.AzureBlobStorageTestAccount.CreateOptions;

public class TestASVUriAndConfiguration {

  private static final int FILE_SIZE = 4096;
  private static final String PATH_DELIMITER = "/";

  protected String accountName;
  protected String accountKey;
  protected static Configuration conf = null;

  private AzureBlobStorageTestAccount testAccount;


  @After
  public void tearDown() throws Exception {
    if (testAccount != null) {
      testAccount.cleanup();
      testAccount = null;
    }
  }

  private boolean validateIOStreams(Path filePath) throws IOException {
    // Capture the file system from the test account.
    //
    FileSystem fs = testAccount.getFileSystem();
    return validateIOStreams(fs, filePath);
  }

  private boolean validateIOStreams(FileSystem fs, Path filePath) throws IOException {

    // Create and write a file
    //
    OutputStream outputStream = fs.create(filePath);
    outputStream.write(new byte[FILE_SIZE]);
    outputStream.close();

    // Return true if the the count is equivalent to the file size.
    //
    return (FILE_SIZE == readInputStream(fs, filePath));
  }

  private int readInputStream(Path filePath) throws IOException {
    // Capture the file system from the test account.
    //
    FileSystem fs = testAccount.getFileSystem();
    return readInputStream(fs, filePath);
  }

  private int readInputStream(FileSystem fs, Path filePath) throws IOException {
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
  @Test
  public void testConnectUsingKey() throws Exception {

    testAccount = AzureBlobStorageTestAccount.create();
    assumeNotNull(testAccount);

    // Validate input and output on the connection.
    //
    assertTrue(validateIOStreams(new Path("/asv_scheme")));
  }

  @Test
  public void testConnectUsingSAS() throws Exception {
    // Create the test account with SAS credentials.
    //
    testAccount = AzureBlobStorageTestAccount.create("",
        EnumSet.of(CreateOptions.UseSas, CreateOptions.CreateContainer));
    // Validate input and output on the connection.
    // NOTE: As of 4/15/2013, Azure Storage has a deficiency that prevents the
    // full scenario from working (CopyFromBlob doesn't work with SAS), so
    // just do a minor check until that is corrected.
    //
    assertFalse(testAccount.getFileSystem().exists(new Path("/IDontExist")));
    //assertTrue(validateIOStreams(new Path("/sastest.txt")));
  }

  @Test
  public void testConnectUsingAnonymous() throws Exception {

    // Create test account with anonymous credentials
    //
    testAccount = AzureBlobStorageTestAccount.createAnonymous("testAsv.txt", FILE_SIZE);
    assumeNotNull(testAccount);

    // Read the file from the public folder using anonymous credentials.
    //
    assertEquals(FILE_SIZE, readInputStream(new Path("/testAsv.txt")));
  }

  @Test
  public void testConnectToEmulator() throws Exception {
    testAccount = AzureBlobStorageTestAccount.createForEmulator();
    assumeNotNull(testAccount);
    assertTrue(validateIOStreams(new Path("/testFile")));
  }

  /**
   * Tests that we can connect to fully qualified accounts outside
   * of blob.core.windows.net
   */
  @Test
  public void testConnectToFullyQualifiedAccountMock() throws Exception {
    Configuration conf = new Configuration();
    AzureBlobStorageTestAccount.setMockAccountKey(conf,
        "mockAccount.mock.authority.net");
    AzureNativeFileSystemStore store = new AzureNativeFileSystemStore();
    MockStorageInterface mockStorage = new MockStorageInterface();
    store.setAzureStorageInteractionLayer(mockStorage);
    NativeAzureFileSystem fs = new NativeAzureFileSystem(store);
    fs.initialize(new URI(
        "asv://mockContainer@mockAccount.mock.authority.net"),
        conf);
    fs.createNewFile(new Path("/x"));
    assertTrue(mockStorage.getBackingStore().exists(
        "http://mockAccount.mock.authority.net/mockContainer/x"));
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
    assumeNotNull(testAccount);


    // Read the file from the default container.
    //
    assertEquals(FILE_SIZE, readInputStream(new Path(PATH_DELIMITER + inblobName)));

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

  /**
   * Creates a file and writes a single byte with the given value in it.
   */
  private static void writeSingleByte(FileSystem fs, Path testFile, int toWrite)
      throws Exception {
    OutputStream outputStream = fs.create(testFile);
    outputStream.write(toWrite);
    outputStream.close();
  }

  /**
   * Reads the file given and makes sure that it's a single-byte file with
   * the given value in it.
   */
  private static void assertSingleByteValue(FileSystem fs, Path testFile,
      int expectedValue) throws Exception {
    InputStream inputStream = fs.open(testFile);
    int byteRead = inputStream.read();
    assertTrue("File unexpectedly empty: " + testFile, byteRead >= 0);
    assertTrue("File has more than a single byte: " + testFile,
        inputStream.read() < 0);
    inputStream.close();
    assertEquals("Unxpected content in: " + testFile,
        expectedValue, byteRead);
  }

  @Test
  public void testMultipleContainers() throws Exception {
    AzureBlobStorageTestAccount
      firstAccount = AzureBlobStorageTestAccount.create("first"),
      secondAccount = AzureBlobStorageTestAccount.create("second");
    assumeNotNull(testAccount);
    assumeNotNull(secondAccount);
    try {
      FileSystem firstFs = firstAccount.getFileSystem(),
          secondFs = secondAccount.getFileSystem();
      Path testFile = new Path("/testAsv");
      assertTrue(validateIOStreams(firstFs, testFile));
      assertTrue(validateIOStreams(secondFs, testFile));
      // Make sure that we're really dealing with two file systems here.
      writeSingleByte(firstFs, testFile, 5);
      writeSingleByte(secondFs, testFile, 7);
      assertSingleByteValue(firstFs, testFile, 5);
      assertSingleByteValue(secondFs, testFile, 7);
    } finally {
      firstAccount.cleanup();
      secondAccount.cleanup();
    }
  }
  
  public void testDefaultKeyProvider() throws Exception {
    Configuration conf = new Configuration();
    String account = "testacct";
    String key = "testkey";
    
    conf.set(SimpleKeyProvider.KEY_ACCOUNT_KEY_PREFIX + account, key);
    
    String result =
        AzureNativeFileSystemStore.getAccountKeyFromConfiguration(account, conf);
    assertEquals(key, result);
  }
  
  public void testValidKeyProvider() throws Exception {
    Configuration conf = new Configuration();
    String account = "testacct";
    String key = "testkey";
    
    conf.set(SimpleKeyProvider.KEY_ACCOUNT_KEY_PREFIX + account, key);
    conf.setClass("fs.azure.account.keyprovider." + account,
        SimpleKeyProvider.class, KeyProvider.class);
    String result =
        AzureNativeFileSystemStore.getAccountKeyFromConfiguration(account, conf);
    assertEquals(key, result);
  }  
  
  public void testInvalidKeyProviderNonexistantClass() throws Exception {
    Configuration conf = new Configuration();
    String account = "testacct";
    
    conf.set("fs.azure.account.keyprovider." + account,
        "org.apache.Nonexistant.Class");
    try {
      AzureNativeFileSystemStore.getAccountKeyFromConfiguration(account, conf);
      Assert.fail("Nonexistant key provider class should have thrown a " +
          "KeyProviderException");
    } catch (KeyProviderException e) {
    }
  }
  
  public void testInvalidKeyProviderWrongClass() throws Exception {
    Configuration conf = new Configuration();
    String account = "testacct";
    
    conf.set("fs.azure.account.keyprovider." + account, "java.lang.String");
    try {
      AzureNativeFileSystemStore.getAccountKeyFromConfiguration(account, conf);
      Assert.fail("Key provider class that doesn't implement KeyProvider " +
          "should have thrown a KeyProviderException");
    } catch (KeyProviderException e) {
    }
  }
}
