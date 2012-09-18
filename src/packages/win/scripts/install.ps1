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

param(
	[String]
	[Parameter( Position=0, Mandatory=$true )]
	$username,
	[String]
	[Parameter( Position=1, Mandatory=$true )]
	$password,
	[String]
	$hdfsRole,
	[String]
	$mapredRole,
	[Switch]
	$skipNamenodeFormat
	)

function Main( $scriptDir )
{
	$HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "hadoop-@version@.winpkg.log"
	Test-JavaHome

	### $hadoopInstallDir: the directory that contains the appliation, after unzipping
	$hadoopInstallDir = Join-Path "$ENV:HADOOP_NODE_INSTALL_ROOT" "hadoop-@version@"
	$hadoopInstallBin = Join-Path "$hadoopInstallDir" "bin"

	Write-Log "HadoopInstallDir: $hadoopInstallDir"
	Write-Log "HadoopInstallBin: $hadoopInstallBin" 

	Write-Log "Username: $username"
	Write-Log "HdfsRole: $hdfsRole"
	Write-Log "MapRedRole: $mapRedRole"
	Write-Log "Ensuring elevated user"

	###
	### Root directory used for HDFS dn and nn files, and for mapdred local dir
	###

	if( -not (Test-Path ENV:HDFS_DATA_DIR))
	{
		$ENV:HDFS_DATA_DIR = Join-Path "$ENV:HADOOP_NODE_INSTALL_ROOT" "HDFS"
	}

	###
	### Begin install
	###
	Write-Log "Installing Apache Hadoop hadoop-@version@ to $ENV:HADOOP_NODE_INSTALL_ROOT"

	if( -not (Test-Path ENV:MASTER_HDFS))
	{
		$ENV:MASTER_HDFS = "namenode datanode secondarynamenode"
	}

	if( -not (Test-Path ENV:MASTER_MR))
	{
		$ENV:MASTER_MR = "jobtracker tasktracker"
	}

	if( -not (Test-Path ENV:SLAVE_MR))
	{
		$ENV:SLAVE_MR = "tasktracker"
	}

	if( -not (Test-Path ENV:ONEBOX_HDFS))
	{
		$ENV:ONEBOX_HDFS = "namenode datanode secondarynamenode"
	}

	if( -not (Test-Path ENV:ONEBOX_MR))
	{
		$ENV:ONEBOX_MR = "jobtracker tasktracker historyserver"
	}

	if( $hdfsRole -eq $null )
	{
		$hdfsRole = "ONEBOX_HDFS"
	}

	if( $mapredRole -eq $null )
	{
		$mapredRole = "ONEBOX_MR"
	}

	###
	###  Unzip Hadoop distribution from compressed archive
	###

	Write-Log "Extracting Hadoop Core archive into $hadoopInstallDir"
	$unzipExpr = "$ENV:WINPKG_BIN\winpkg.ps1 `"$HDP_RESOURCES_DIR\hadoop-@version@.zip`" utils unzip `"$ENV:HADOOP_NODE_INSTALL_ROOT`""
	Invoke-Ps $unzipExpr
	
	foreach( $template in (Get-ChildItem "$HDP_INSTALL_PATH\..\template\conf\*.xml"))
	{
		try
		{
			Write-Log "Copying XML template from $($template.FullName)"
			Copy-XmlTemplate $template.FullName "$hadoopInstallDir\conf"
		}
		catch [Exception] 
		{
			Write-Log $_.Exception.Message $_
		}
	}

	$xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template\conf\*.properties`" `"$hadoopInstallDir\bin`""
	Invoke-Cmd $xcopy_cmd

	$xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template\bin`" `"$hadoopInstallDir\bin`""
	Invoke-Cmd $xcopy_cmd

	###
	### Grant Hadoop user access to HADOOP_INSTALL_DIR and HDFS Root
	###
	$cmd = "icacls `"$hadoopInstallDir`" /grant ${username}:(OI)(CI)F"
	Invoke-Cmd $cmd

	if( -not (Test-Path $ENV:HDFS_DATA_DIR))
	{
		Write-Log "Creating HDFS Data Directory $ENV:HDFS_DATA_DIR"
		mkdir $ENV:HDFS_DATA_DIR
	}

	$cmd = "icacls `"$ENV:HDFS_DATA_DIR`" /grant ${username}:(OI)(CI)F"
	Invoke-Cmd $cmd


	###
	### Create symlink for streaming jar
	###
	Write-Log "Creating Symlink for Streaming to $hadoopInstallDir\contrib\streaming\hadoop-streaming-@version@.jar"
	$symlinkStreaming_cmd = "mklink `"$hadoopInstallDir\lib\hadoop-streaming.jar`" `"$hadoopInstallDir\contrib\streaming\hadoop-streaming-@version@.jar`""
	Invoke-Cmd $symlinkStreaming_cmd

	###
	### Create Hadoop Windows Services and grant user ACLS to start/stop
	###

	$securePassword = ConvertTo-SecureString $password -AsPlainText -Force
	$serviceCredentials = New-Object System.Management.Automation.PSCredential ("$ENV:COMPUTERNAME\$username", $securePassword)

	$hdfsRoleServices = $executionContext.InvokeCommand.ExpandString( "`$ENV:$hdfsRole" )
	$mapRedRoleServices = $executionContext.InvokeCommand.ExpandString( "`$ENV:$mapredRole" )

	Write-Log "HdfsRole: $hdfsRole"	Write-Log "MapRedRole: $mapRedRole"
	Write-Log "Node HDFS Role Services: $hdfsRoleServices"
	Write-Log "Node MAPRED Role Services: $mapRedRoleServices"

	$allServices = $hdfsRoleServices + " " + $mapRedRoleServices

	Write-Log "Installing services $allServices"

	foreach( $service in $allServices.Split(' '))
	{
		try
		{
			Write-Log "Creating service $service as $hadoopInstallBin\$service.exe"
			Copy-Item "$HDP_RESOURCES_DIR\serviceHost.exe" "$hadoopInstallBin\$service.exe" -Force

			#serviceHost.exe will write to this log but does not create it
			#Creating the event log needs to be done from an elevated process, so we do it here
			if( -not ([Diagnostics.EventLog]::SourceExists( "$service" )))
			{
				[Diagnostics.EventLog]::CreateEventSource( "$service", "" )
			}

			Write-Log( "Adding service $service" )
			$s = New-Service -Name "$service" -BinaryPathName "$hadoopInstallBin\$service.exe" -Credential $serviceCredentials -DisplayName "Apache Hadoop $service"

			$cmd="$ENV:WINDIR\system32\sc.exe failure $service reset= 30 actions= restart/5000"
			Invoke-Cmd $cmd

			$cmd="$ENV:WINDIR\system32\sc.exe config $service start= auto"
			Invoke-Cmd $cmd

			Set-ServiceAcl $service
		}
		catch [Exception]
		{
			Write-Log $_.Exception.Message $_
		}
	}

	###
	### Setup HDFS service config
	###

	Write-Log "Copying configuration for $hdfsRoleServices"

	foreach( $service in $hdfsRoleServices.Split( ' ' ))
	{
		Write-Log "Creating service config ${hadoopInstallBin}\${service}.xml"
		$cmd = "$hadoopInstallBin\hdfs.cmd --service $service > `"$hadoopInstallBin\$service.xml`""
		Invoke-Cmd $cmd		
	}

	###
	### Setup MapRed service config
	### 

	foreach( $service in $mapRedRoleServices.Split( ' ' ))
	{
		Write-Log "Creating service config $hadoopInstallBin\$service.xml"
		$cmd = "$hadoopInstallBin\mapred.cmd --service $service > `"$hadoopInstallBin\$service.xml`""
		Invoke-Cmd $cmd		
	}

	if ($skipNamenodeFormat -ne $true) 
	{
		###
		### Format the namenode
		###
		Write-Log "Formatting HDFS"
		
		$cmd="$hadoopInstallBin\hadoop.cmd namenode -format"
		Invoke-Cmd $cmd
	}
	else 
	{
		Write-Log "Skipping HDFS format"
	}
	
	Write-Log "Installation of Hadoop Core complete"
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