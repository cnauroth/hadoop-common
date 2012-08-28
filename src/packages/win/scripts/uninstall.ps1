### Licensed to the Apache Software Foundation (ASF) under one or more
### contributor license agreements.  See the NOTICE file distributed with
### this work for additional information regarding copyright ownership.
### The ASF licenses this file to You under the Apache License, Version 2.0
### (the "License"); you may not use this file except in compliance with
### the License.  You may obtain a copy of the License at
###
###     http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.


function Main
{
	$HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "hadoop-@version@.winpkg.log" $ENV:WINPKG_BIN


	### $hadoopInstallDir: the directory that contains the appliation, after unzipping
	$hadoopInstallDir = Join-Path "$ENV:HADOOP_NODE_INSTALL_ROOT" "hadoop-@version@"
	$hadoopInstallBin = Join-Path "$hadoopInstallDir" "bin"

	###
	### Root directory used for HDFS dn and nn files, and for mapdred local dir
	###

	if( -not (Test-Path ENV:HDFS_DATA_DIR))
	{
		$ENV:HDFS_DATA_DIR = "$ENV:HADOOP_NODE_INSTALL_ROOT\HDFS"
	}

	###
	### Stop and delete services
	###

	foreach( $service in ("namenode", "datanode", "secondarynamenode", "jobtracker", "tasktracker", "historyserver"))
	{
		Write-Log "Stopping $service"
		$s = Get-Service $service -ErrorAction SilentlyContinue 

		if( $s -ne $null )
		{
			Stop-Service $service
			$cmd = "sc.exe delete $service"
			Invoke-Cmd $cmd
		}
	}

	Write-Log "Removing HDFS_DATA_DIR ($HDFS_DATA_DIR)"
	$cmd = "rd /s /q $ENV:HDFS_DATA_DIR"
	Invoke-Cmd $cmd

	Write-Log "Removing Hadoop ($hadoopInstallDir)"
	$cmd = "rd /s /q $hadoopInstallDir"
	Invoke-Cmd $cmd
}

try
{
	$scriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)
	$utilsModule = Import-Module -Name "$scriptDir\..\resources\Winpkg.Utils.psm1" -ArgumentList ("HADOOP") -PassThru
	Main $scriptDir
}
finally
{
	if( $utilsModule -ne $null )
	{
		Remove-Module $utilsModule
	}
}