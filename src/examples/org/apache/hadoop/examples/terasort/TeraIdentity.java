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

import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Generates the sampled split points, launches the job, and waits for it to
 * finish. 
 * <p>
 * To run the program: 
 * <b>bin/hadoop jar hadoop-examples-*.jar teraidentity in-dir out-dir</b>
 */
public class TeraIdentity extends Configured implements Tool 
{
	private static final Log LOG = LogFactory.getLog(TeraIdentity.class);

  
	public int run(String[] args) throws Exception 
	{
		LOG.info("starting TeraIdentity Job");
		JobConf job = (JobConf) getConf();
		Path inputDir = new Path(args[0]);
		inputDir = inputDir.makeQualified(inputDir.getFileSystem(job));
		TeraInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setJobName("TeraIdentity");
		job.setJarByClass(TeraSort.class);
	    job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormat(TeraIdentityInputFormat.class);
		job.setOutputFormat(TeraOutputFormat.class);
		TeraOutputFormat.setFinalSync(job, true);
		JobClient.runJob(job);
		LOG.info("done");
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new JobConf(), new TeraIdentity(), args);
		System.exit(res);
	}
}
