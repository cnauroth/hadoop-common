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
package org.apache.hadoop.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An input stream which supports {@link #position()}. 
 */
public class PositionInputStream extends FilterInputStream {
  public static final Log LOG = LogFactory.getLog(PositionInputStream.class);

  private long currPos = 0;
  private long markPos = -1;

  public PositionInputStream(InputStream in) {
    super(in);
  }

  /**
   * @return the current position
   */
  public long position() {
    return currPos;
  }
  
  @Override
  public int read() throws IOException {
    final int b = super.read();
    if (b != -1) {
      currPos++;
    }
    return b;
  }

  @Override
  public int read(byte[] buffer) throws IOException {
    return read(buffer, 0, buffer.length);
  }

  @Override
  public int read(byte[] buffer, int offset, int length) throws IOException {
    final int n = super.read(buffer, offset, length);
    if (n > 0) {
      currPos += n;
    }
    return n;
  }
  
  @Override
  public long skip(long length) throws IOException {
    final long n = super.skip(length);
    if (n > 0) {
      currPos += n;
    }
    return n;
  }

  @Override
  public void mark(int readlimit) {
    super.mark(readlimit);
    markPos = currPos;

    if (LOG.isDebugEnabled()) {
      LOG.debug("mark: markPos=" + markPos + ", readlimit=" + readlimit);
    }
  }

  @Override
  public void reset() throws IOException {
    if (markPos == -1) {
      throw new IOException("Cannot reset without mark.");
    }
    super.reset();
    currPos = markPos;
    markPos = -1;

    if (LOG.isDebugEnabled()) {
      LOG.debug("reset: currPos=" + currPos);
    }
  }
}
