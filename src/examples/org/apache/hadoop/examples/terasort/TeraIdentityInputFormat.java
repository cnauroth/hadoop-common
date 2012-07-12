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

package org.apache.hadoop.examples.terasort;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * An input format that reads the first 10 characters of each line as the key
 * and the rest of the line as the value. Both key and value are represented
 * as Text.
 */
@SuppressWarnings("deprecation")
public class TeraIdentityInputFormat extends FileInputFormat<Text,Text> {

  private static JobConf lastConf = null;
  private static InputSplit[] lastResult = null;
  

  static class TeraRecordReader implements RecordReader<Text,Text> {
    private LineRecordReader in;
    private LongWritable junk = new LongWritable();
    private Text line = new Text();
    private static int KEY_LENGTH = 10;

    public TeraRecordReader(Configuration job, 
                            FileSplit split) throws IOException {
      in = new LineRecordReader(job, split);
    }

    public void close() throws IOException {
      in.close();
    }

    public Text createKey() {
      return new Text();
    }

    public Text createValue() {
      return new Text();
    }

    public long getPos() throws IOException {
      return in.getPos();
    }

    public float getProgress() throws IOException {
      return in.getProgress();
    }

    public boolean next(Text key, Text value) throws IOException {
      if (in.next(junk, line)) {
        if (line.getLength() < KEY_LENGTH) {
          key.set(line);
          value.clear();
        } else {
          byte[] bytes = line.getBytes();
          key.set(bytes, 0, KEY_LENGTH);
          value.set(bytes, KEY_LENGTH, line.getLength() - KEY_LENGTH);
        }
        return true;
      } else {
        return false;
      }
    }
  }

  @Override
  public RecordReader<Text, Text> 
      getRecordReader(InputSplit split,
                      JobConf job, 
                      Reporter reporter) throws IOException {
    return new TeraRecordReader(job, (FileSplit) split);
  }

  @Override
  public InputSplit[] getSplits(JobConf conf, int splits) throws IOException {
    if (conf == lastConf) {
      return lastResult;
    }
    lastConf = conf;
    lastResult = super.getSplits(conf, splits);
    return lastResult;
  }
}
