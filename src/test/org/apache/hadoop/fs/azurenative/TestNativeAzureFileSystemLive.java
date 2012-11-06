package org.apache.hadoop.fs.azurenative;

import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.blob.client.*;

import java.net.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.*;

import junit.framework.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.*;

/*
 * Tests the Native Azure file system (ASV) against an actual blob store if provided in the environment.
 */
public class TestNativeAzureFileSystemLive extends TestCase {
  private static final String CONNECTION_STRING_PROPERTY_NAME = "fs.azure.storageConnectionString";
  private FileSystem fs;
  private CloudBlobContainer container;

  private static boolean hasConnectionString(Configuration conf) {
    if (conf.get(CONNECTION_STRING_PROPERTY_NAME) != null) {
      return true;
    }
    if (System.getenv(CONNECTION_STRING_PROPERTY_NAME) != null) {
      conf.set(CONNECTION_STRING_PROPERTY_NAME,
          System.getenv(CONNECTION_STRING_PROPERTY_NAME));
      return true;
    }

    return false;
  }

  @Override
  protected void setUp() throws Exception {
    fs = null;
    container = null;
    Configuration conf = new Configuration();
    if (!hasConnectionString(conf)) {
      System.out
          .println("Skipping live Azure test because of missing connection string.");
      return;
    }
    fs = new NativeAzureFileSystem();
    String containerName = String.format("asvtests-%s-%tQ",
        System.getProperty("user.name"), new Date());
    String connectionString = conf.get(CONNECTION_STRING_PROPERTY_NAME);
    CloudStorageAccount account = CloudStorageAccount
        .parse(connectionString);
    container = account
        .createCloudBlobClient().getContainerReference(containerName);
    container.create();
    String accountUrl = account.getBlobEndpoint().getAuthority();
    String accountName = accountUrl.substring(0, accountUrl.indexOf('.'));
    conf.set(CONNECTION_STRING_PROPERTY_NAME + "." + accountName, connectionString);
    fs.initialize(
        new URI("asv://" + accountUrl + "/" + containerName + "/"),
        conf);
  }

  @Override
  protected void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (container != null) {
      container.delete();
      container = null;
    }
  }

  public void testStoreRetrieveFile() throws Exception {
    if (fs == null)
      return;
    Path testFile = new Path("unit-test-file");
    writeString(testFile, "Testing");
    assertTrue(fs.exists(testFile));
    assertEquals("Testing", readString(testFile));
    fs.delete(testFile, true);
  }

  public void testStoreDeleteFolder() throws Exception {
    if (fs == null)
      return;
    Path testFolder = new Path("storeDeleteFolder");
    assertFalse(fs.exists(testFolder));
    assertTrue(fs.mkdirs(testFolder));
    assertTrue(fs.exists(testFolder));
    Path innerFile = new Path(testFolder, "innerFile");
    assertTrue(fs.createNewFile(innerFile));
    assertTrue(fs.exists(innerFile));
    assertTrue(fs.delete(testFolder, true));
    assertFalse(fs.exists(innerFile));
    assertFalse(fs.exists(testFolder));
  }

  public void testFileOwnership() throws Exception {
    if (fs == null)
      return;
    Path testFile = new Path("ownershipTestFile");
    writeString(testFile, "Testing");
    testOwnership(testFile);
  }

  public void testFolderOwnership() throws Exception {
    if (fs == null)
      return;
    Path testFolder = new Path("ownershipTestFolder");
    fs.mkdirs(testFolder);
    testOwnership(testFolder);
  }

  private void testOwnership(Path pathUnderTest) throws IOException {
    FileStatus ret = fs.getFileStatus(pathUnderTest);
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    assertTrue(ret.isOwnedByUser(currentUser.getShortUserName(), currentUser.getGroupNames()));
    fs.delete(pathUnderTest, true);
  }

  public void testFilePermissions() throws Exception {
    if (fs == null)
      return;
    Path testFile = new Path("permissionTestFile");
    FsPermission permission = FsPermission.createImmutable((short) 644);
    createEmptyFile(testFile, permission);
    FileStatus ret = fs.getFileStatus(testFile);
    assertEquals(permission, ret.getPermission());
    fs.delete(testFile, true);
  }

  public void testFolderPermissions() throws Exception {
    if (fs == null)
      return;
    Path testFolder = new Path("permissionTestFolder");
    FsPermission permission = FsPermission.createImmutable((short) 644);
    fs.mkdirs(testFolder, permission);
    FileStatus ret = fs.getFileStatus(testFolder);
    assertEquals(permission, ret.getPermission());
    fs.delete(testFolder, true);
  }

  public void testDeepFileCreation() throws Exception {
    if (fs == null)
      return;
    Path testFile = new Path("deep/file/creation/test");
    FsPermission permission = FsPermission.createImmutable((short) 644);
    createEmptyFile(testFile, permission);
    assertTrue(fs.exists(testFile));
    assertTrue(fs.exists(new Path("deep")));
    assertTrue(fs.exists(new Path("deep/file/creation")));
    FileStatus ret = fs.getFileStatus(new Path("deep/file"));
    assertTrue(ret.isDir());
    assertEquals(permission, ret.getPermission());
    assertTrue(fs.delete(new Path("deep"), true));
    assertFalse(fs.exists(testFile));

    // An alternative test scenario would've been to delete the file first,
    // and then check for the existence of the upper folders still. But that
    // doesn't actually work as expected right now.
  }

  // This doesn't work right now... (Authorization exception)
  // public void testGlobalizedFileNames() throws Exception {
  // if (fs == null) return;
  // Path testFile = new Path("┘ç╪»┘ê╪¿");
  // assertTrue(fs.createNewFile(testFile));
  // assertTrue(fs.exists(testFile));
  // assertTrue(fs.delete(testFile, true));
  // }

  private static enum RenameVariation {
    NormalFileName, SourceInAFolder, SourceWithSpace, SourceWithPlusAndPercent
  }

  public void testRename() throws Exception {
    if (fs == null)
      return;
    for (RenameVariation variation : RenameVariation.values()) {
      System.out.printf("Rename variation: %s\n", variation);
      Path originalFile;
      switch (variation) {
      case NormalFileName:
        originalFile = new Path("fileToRename");
        break;
      case SourceInAFolder:
        originalFile = new Path("file/to/rename");
        break;
      case SourceWithSpace:
        originalFile = new Path("file to rename");
        break;
      case SourceWithPlusAndPercent:
        originalFile = new Path("file+to%rename");
        break;
      default:
        throw new Exception("Unknown variation");
      }
      Path destinationFile = new Path("file/resting/destination");
      assertTrue(fs.createNewFile(originalFile));
      assertTrue(fs.exists(originalFile));
      assertFalse(fs.rename(originalFile, destinationFile)); // Parent directory
                                                             // doesn't exist
      assertTrue(fs.mkdirs(destinationFile.getParent()));
      assertTrue(fs.rename(originalFile, destinationFile));
      assertTrue(fs.exists(destinationFile));
      assertFalse(fs.exists(originalFile));
      fs.delete(destinationFile.getParent(), true);
    }
  }

  private static enum RenameFolderVariation {
    CreateFolderAndInnerFile, CreateJustInnerFile, CreateJustFolder
  }

  public void testRenameFolder() throws Exception {
    if (fs == null)
      return;
    for (RenameFolderVariation variation : RenameFolderVariation.values()) {
      Path originalFolder = new Path("folderToRename");
      if (variation != RenameFolderVariation.CreateJustInnerFile)
        assertTrue(fs.mkdirs(originalFolder));
      Path innerFile = new Path(originalFolder, "innerFile");
      if (variation != RenameFolderVariation.CreateJustFolder)
        assertTrue(fs.createNewFile(innerFile));
      Path destination = new Path("renamedFolder");
      assertTrue(fs.rename(originalFolder, destination));
      assertTrue(fs.exists(destination));
      if (variation != RenameFolderVariation.CreateJustFolder)
        assertTrue(fs.exists(new Path(destination, innerFile.getName())));
      assertFalse(fs.exists(originalFolder));
      assertFalse(fs.exists(innerFile));
      fs.delete(destination, true);
    }
  }

  public void testCopyFromLocalFileSystem() throws Exception {
    if (fs == null)
      return;
    Path localFilePath = new Path(System.getProperty("test.build.data",
        "azure_test"));
    FileSystem localFs = FileSystem.get(new Configuration());
    localFs.delete(localFilePath, true);
    try {
      writeString(localFs, localFilePath, "Testing");
      Path dstPath = new Path("copiedFromLocal");
      assertTrue(FileUtil.copy(localFs, localFilePath, fs, dstPath, false,
          fs.getConf()));
      assertTrue(fs.exists(dstPath));
      assertEquals("Testing", readString(fs, dstPath));
      fs.delete(dstPath, true);
    } finally {
      localFs.delete(localFilePath, true);
    }
  }

  public void testStatistics() throws Exception {
    if (fs == null)
      return;

    FileSystem.clearStatistics();
    FileSystem.Statistics stats = FileSystem.getStatistics("asv", NativeAzureFileSystem.class);
    assertEquals(0, stats.getBytesRead());
    assertEquals(0, stats.getBytesWritten());
    Path newFile = new Path("testStats");
    writeString(newFile, "12345678");
    assertEquals(8, stats.getBytesWritten());
    assertEquals(0, stats.getBytesRead());
    String readBack = readString(newFile);
    assertEquals("12345678", readBack);
    assertEquals(8, stats.getBytesRead());
    assertEquals(8, stats.getBytesWritten());
    assertTrue(fs.delete(newFile, true));
    assertEquals(8, stats.getBytesRead());
    assertEquals(8, stats.getBytesWritten());
  }

  private void createEmptyFile(Path testFile, FsPermission permission)
      throws IOException {
    FSDataOutputStream outputStream = fs.create(testFile, permission, true,
        4096, (short) 1, 1024, null);
    outputStream.close();
  }

  private String readString(Path testFile) throws IOException {
    return readString(fs, testFile);
  }

  private String readString(FileSystem fs, Path testFile) throws IOException {
    FSDataInputStream inputStream = fs.open(testFile);
    String ret = readString(inputStream);
    inputStream.close();
    return ret;
  }

  private String readString(FSDataInputStream inputStream) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(
        inputStream));
    final int BUFFER_SIZE = 1024;
    char buffer[] = new char[BUFFER_SIZE];
    int count = reader.read(buffer, 0, BUFFER_SIZE);
    if (count >= BUFFER_SIZE) {
      throw new IOException("Exceeded buffer size");
    }
    inputStream.close();
    return new String(buffer, 0, count);
  }

  private void writeString(Path path, String value) throws IOException {
    writeString(fs, path, value);
  }

  private void writeString(FileSystem fs, Path path, String value)
      throws IOException {
    FSDataOutputStream outputStream = fs.create(path, true);
    writeString(outputStream, value);
    outputStream.close();
  }

  private void writeString(FSDataOutputStream outputStream, String value)
      throws IOException {
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
        outputStream));
    writer.write(value);
    writer.close();
  }
}
