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

function Write-Log ($message, $level, $pipelineObj)
{
	switch($level)
	{
		"Failure" {
			$message = "HADOOP FAILURE: $message"
			Write-Host $message
			break;
		}

		"Info" {
			$message = "HADOOP: $message"
			Write-Host $message
			break;
		}

		default	{
			$message = "HADOOP: $message"
			Write-Verbose "$message"
		}
	}

	Out-File -FilePath $ENV:WINPKG_LOG -InputObject "$message" -Append -Encoding "UTF8"

    if( $pipelineObj -ne $null )
    {
        Out-File -FilePath $ENV:WINPKG_LOG -InputObject $pipelineObj.InvocationInfo.PositionMessage -Append -Encoding "UTF8"
    }
}

function Execute-Cmd ($command)
{
	Write-Log $command
	cmd.exe /C "$command"
}

function Execute-Ps ($command)
{
	Write-Log $command
	Invoke-Expression "$command"
}
try
{
	if( -not (Test-Path ENV:HADOOP_NODE_INSTALL_ROOT))
	{
		$ENV:HADOOP_NODE_INSTALL_ROOT = "c:\hadoop"
	}

	$HDP_INSTALL_PATH = Split-Path $MyInvocation.MyCommand.Path
	$HDP_RESOURCES_DIR = Resolve-Path "$HDP_INSTALL_PATH\..\resources"
	

	if( -not (Test-Path ENV:WINPKG_LOG ))
	{
		$ENV:WINPKG_LOG="$ENV:HADOOP_NODE_INSTALL_ROOT\@hadoop.core.winpkg@.log"
		Write-Log "Logging to $ENV:WINPKG_LOG"
	}

	### $hadoopInstallDir: the directory that contains the appliation, after unzipping
	$hadoopInstallDir = Join-Path "$ENV:HADOOP_NODE_INSTALL_ROOT" "hadoop-@version@"
	$hadoopInstallBin = Join-Path "$hadoopInstallDir" "bin"

	Write-Log "Ensuring elevated user"

	$currentPrincipal = New-Object Security.Principal.WindowsPrincipal( [Security.Principal.WindowsIdentity]::GetCurrent( ) )
	if ( -not ($currentPrincipal.IsInRole( [Security.Principal.WindowsBuiltInRole]::Administrator ) ) )
	{
		Write-Log "$ENV:WINPKG_LOG FATAL ERROR: install script must be run elevated"
		exit 1
	} 

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

	foreach( $service in ("namenode", "datanode", "secondarynamenode", "jobtracker", "tasktracker"))
	{
		Write-Log "Stopping $service"
		$s = Get-Service $service -ErrorAction SilentlyContinue 

		if( $s -ne $null )
		{
			Stop-Service $service
			$cmd = "sc.exe delete $service"
			Execute-Cmd $cmd
		}
	}

	Write-Log "Removing HDFS_DATA_DIR ($HDFS_DATA_DIR)"
	$cmd = "rd /s /q $ENV:HDFS_DATA_DIR"
	Execute-Cmd $cmd

	Write-Log "Removing Hadoop ($hadoopInstallDir)"
	$cmd = "rd /s /q $hadoopInstallDir"
	Execute-Cmd $cmd
}
catch [Exception]
{
	Write-Log $_.Exception.Message "Failure" $_
}
finally
{
}