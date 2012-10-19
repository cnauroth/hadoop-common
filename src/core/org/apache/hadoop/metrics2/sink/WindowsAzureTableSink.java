/* Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.metrics2.sink;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.Metric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.log4j.Logger;

import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.table.client.*;

/**
 * This sink writes the metrics to a Windows Azure Table store. 
 * This depends on the Windows Azure SDK for Java version 0.2.2
 * http://go.microsoft.com/fwlink/?LinkID=236226&clcid=0x409
 * 
 * The jar file also has the following dependencies:
 * http://www.windowsazure.com/en-us/develop/java/java-home/download-the-windows-azure-sdk-for-java/
 *    commons-lang3-3.1.jar
 *    commons-logging-1.1.1.jar
 *    jackson-core-asl-1.8.3.jar
 *    jackson-jaxrs-1.8.3.jar
 *    jackson-mapper-asl-1.8.3.jar
 *    jackson-xc-1.8.3.jar
 *    javax.inject-1.jar
 *    jaxb-impl-2.2.3-1.jar
 *    jersey-client-1.10-b02.jar
 *    jersey-core-1.10-b02.jar
 *    jersey-json-1.10-b02.jar
 *    jettison-1.1.jar
 *    stax-api-1.0.1.jar
 *    javax.mail.jar
 * 
 * Table Schema:
 * A new table is created per context and record name combination and the 
 * table is named "CONTEXTrecordname".
 * For example:
 * DFSnamenode, DFSdatanode, DFSFSnamesystem, JVMmetrics, MAPREDjobtracker, MAPREDtasktracker, RPCrpc
 * 
 * The table data is partitioned by hour and the partition number is the hours since January 1, 1970, 00:00:00 GMT 
 * and the row key is a random UUID.
 * 
 * Hadoop-metrics2.properties:
 * The windows azure storage account name and the access key must be passed in through the properties file.
 * 
 * Here is a sample configuration in the hadoop-metrics2.properties file.
 * 		*.sink.table.class=org.apache.hadoop.metrics2.sink.WindowsAzureTableSink
 * 		
 * 		namenode.sink.dfsinstance.class=${*.sink.table.class}
 * 		namenode.sink.dfsinstance.context=dfs
 * 		namenode.sink.dfsinstance.accountname=azure_storage_account_name_here
 * 		namenode.sink.dfsinstance.accesskey=azure_storage_account_key_here
 * 
 */
public class WindowsAzureTableSink implements MetricsSink {

	private static final String STORAGE_ACCOUNT_KEY = "accountname";
	private static final String STORAGE_ACCESSKEY_KEY = "accesskey";
	
	private CloudStorageAccount storageAccount;
	
	private static Logger logger = Logger.getLogger(WindowsAzureTableSink.class);
	
	/*
	 * Contains a list of tables that are created on the Azure table store.
	 * The values are populated when the first metrics arrive.
	 */
	private HashMap<String, CloudTableClient> existingTables = new HashMap<String, CloudTableClient>();
	
	@Override
	public void init(SubsetConfiguration conf) {
		logger.info("Entering init");
		
		String storageAccountName = conf.getString(STORAGE_ACCOUNT_KEY);
		String storageAccountKey = conf.getString(STORAGE_ACCESSKEY_KEY);
		
		String storageConnectionString = "DefaultEndpointsProtocol=http" + 
			    				  		 ";AccountName=" + storageAccountName +
			    				  		 ";AccountKey=" + storageAccountKey;
		
		// Retrieve storage account from connection-string
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
		} catch (InvalidKeyException invalidKeyException) {
			logger.error("Invalid Key for storage account: " + storageAccountName);
		} catch (URISyntaxException uriSyntaxException) {
			logger.error("Invalid URI. Details: " + uriSyntaxException.getMessage());
		}
 	}

	@Override
	public void putMetrics(MetricsRecord record) {
		// table name is of the form CONTEXTrecordname
		// Azure Tables can only contain alpha-numeric values
		String tableName = record.context().toUpperCase() + record.name();
		
		// Calculate the hour part. record.timestamp() is in milliseconds 
		// 1000 milliseconds * 60 seconds * 60 minutes
		long partitionNumber = record.timestamp() / (1000*60*60);
		
		AzureTableMetrics2Entity metrics2Entity = 
				new AzureTableMetrics2Entity(String.valueOf(partitionNumber), 
											 UUID.randomUUID().toString());
		
		HashMap<String, String> metrics2KeyValuePairs = new HashMap<String, String>();
		
		metrics2KeyValuePairs.put("Timestamp", String.valueOf(record.timestamp()));
		metrics2KeyValuePairs.put("Context", record.context());
		metrics2KeyValuePairs.put("Name", record.name());
		
		for (MetricsTag tag : record.tags()) {
			metrics2KeyValuePairs.put(tag.name(), String.valueOf(tag.value()));
		}
		
		for (Metric metric : record.metrics()) {
			metrics2KeyValuePairs.put(metric.name(), metric.value().toString());
		}
		
		metrics2Entity.setMetrics2KeyValuePairs(metrics2KeyValuePairs);
		
		CloudTableClient tableClient;
		
		try {
			tableClient = createTableIfNotExists(tableName);
		} catch (StorageException storageException) {
			logger.error(String.format("createTableIfNotExists failed. Details: %s, %s", 
					storageException.getMessage(), storageException.getStackTrace()));
			
			return;
		} catch (URISyntaxException syntaxException) {
			logger.error(String.format("createTableIfNotExists failed. Details: %s, %s", 
					syntaxException.getMessage(), syntaxException.getStackTrace()));
			
			return;
		}
		
		TableOperation insertMetricOperation = TableOperation.insert(metrics2Entity);
		
		try {
			tableClient.execute(tableName, insertMetricOperation);
		} catch (StorageException storageException) {
			logger.error(String.format("tableClient.execute failed. Details: %s, %s", 
					storageException.getMessage(), storageException.getStackTrace()));
			
			return;
		}
	}

	@Override
	public void flush() {
		// Nothing to do here
	}
	
	/*
	 * Create a windows azure table if one does not already exist.
	 */
	private CloudTableClient createTableIfNotExists(String tableName)
			throws StorageException, URISyntaxException {
		if (existingTables.containsKey(tableName)) {
			return existingTables.get(tableName);
		}
		
		// Create the table client.
		CloudTableClient tableClient = storageAccount.createCloudTableClient();
		 
		// Create the table if it doesn't exist.
		tableClient.getTableReference(tableName).createIfNotExist();
		
		logger.info(String.format("Created table '%s'", tableName));
		
		existingTables.put(tableName, tableClient);
		
		return tableClient;
	}
	
	/*
	 * Table store entity for the metrics2 data.
	 */
	private class AzureTableMetrics2Entity extends TableServiceEntity {
		private HashMap<String, String> metrics2KeyValuePairs;
		
		public AzureTableMetrics2Entity(String partitionKey, String rowKey) {
	        this.partitionKey = partitionKey;
	        this.rowKey = rowKey;
	    }
		
		public void setMetrics2KeyValuePairs(HashMap<String, String> metrics2KeyValuePairs) {
			this.metrics2KeyValuePairs = metrics2KeyValuePairs;
		}
		
		@Override    
		public HashMap<String, EntityProperty> writeEntity(final OperationContext opContext) {   
			final HashMap<String, EntityProperty> retVal = new HashMap<String, EntityProperty>();
			
			if (metrics2KeyValuePairs != null) {
				for (Entry<String, String> keyValuePair : metrics2KeyValuePairs.entrySet()) {
					retVal.put(keyValuePair.getKey(), new EntityProperty(keyValuePair.getValue()));
				}
			}
			
			return retVal;
		}
	}
}