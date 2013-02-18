package org.apache.hadoop.fs.azurenative;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import junit.framework.TestCase;

public class TestNativeAzureFileSystemBlockLocations extends TestCase {
  public void testNumberOfBlocks() throws Exception {
    Configuration conf  = new Configuration();
    conf.set(NativeAzureFileSystem.AZURE_BLOCK_SIZE_PROPERTY_NAME, "500");
    AzureBlobStorageTestAccount testAccount =
        AzureBlobStorageTestAccount.createMock(conf);
    FileSystem fs = testAccount.getFileSystem();
    Path testFile = createTestFile(fs, 1200);
    FileStatus stat = fs.getFileStatus(testFile);
    assertEquals(500, stat.getBlockSize());
    testAccount.cleanup();
  }

  public void testBlockLocationsTypical() throws Exception {
    BlockLocation[] locations = getBlockLocationsOutput(210, 50, 0, 210);
    assertEquals(5, locations.length);
    assertEquals("localhost", locations[0].getHosts()[0]);
    assertEquals(50, locations[0].getLength());
    assertEquals(10, locations[4].getLength());
    assertEquals(100, locations[2].getOffset());
  }

  public void testBlockLocationsEmptyFile() throws Exception {
    BlockLocation[] locations = getBlockLocationsOutput(0, 50, 0, 0);
    assertEquals(0, locations.length);
  }

  public void testBlockLocationsSmallFile() throws Exception {
    BlockLocation[] locations = getBlockLocationsOutput(1, 50, 0, 1);
    assertEquals(1, locations.length);
    assertEquals(1, locations[0].getLength());
  }

  public void testBlockLocationsExactBlockSizeMultiple() throws Exception {
    BlockLocation[] locations = getBlockLocationsOutput(200, 50, 0, 200);
    assertEquals(4, locations.length);
    assertEquals(150, locations[3].getOffset());
    assertEquals(50, locations[3].getLength());
  }

  public void testBlockLocationsSubsetOfFile() throws Exception {
    BlockLocation[] locations = getBlockLocationsOutput(205, 10, 15, 35);
    assertEquals(4, locations.length);
    assertEquals(10, locations[0].getLength());
    assertEquals(15, locations[0].getOffset());
    assertEquals(5, locations[3].getLength());
    assertEquals(45, locations[3].getOffset());
  }

  public void testBlockLocationsOutOfRangeSubsetOfFile() throws Exception {
    BlockLocation[] locations = getBlockLocationsOutput(205, 10, 300, 10);
    assertEquals(0, locations.length);
  }

  public void testBlockLocationsEmptySubsetOfFile() throws Exception {
    BlockLocation[] locations = getBlockLocationsOutput(205, 10, 0, 0);
    assertEquals(0, locations.length);
  }

  private static BlockLocation[] getBlockLocationsOutput(int fileSize,
      int blockSize, long start, long len) throws Exception {
    Configuration conf  = new Configuration();
    conf.set(NativeAzureFileSystem.AZURE_BLOCK_SIZE_PROPERTY_NAME,
        "" + blockSize);
    AzureBlobStorageTestAccount testAccount =
        AzureBlobStorageTestAccount.createMock(conf);
    FileSystem fs = testAccount.getFileSystem();
    Path testFile = createTestFile(fs, fileSize);
    FileStatus stat = fs.getFileStatus(testFile);
    BlockLocation[] locations = fs.getFileBlockLocations(stat, start, len);
    testAccount.cleanup();
    return locations;
  }

  private static Path createTestFile(FileSystem fs, int size)
      throws Exception {
    Path testFile = new Path("/testFile");
    OutputStream outputStream = fs.create(testFile);
    outputStream.write(new byte[size]);
    outputStream.close();
    return testFile;
  }
}
