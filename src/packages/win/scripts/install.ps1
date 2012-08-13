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
	$username,
	$password,
	$hdfsRole,
	$mapredRole
	)


function Write-Log ($message )
{
	Out-File -FilePath $ENV:WINPKG_LOG -InputObject $message -Append -Encoding "UTF8"
	Write-Host $message
}

function Execute-Cmd ($command)
{
	Write-Log $command
	cmd.exe /C "$command"
}

### Add service control permissions to authenticated users.
### Reference:
### http://stackoverflow.com/questions/4436558/start-stop-a-windows-service-from-a-non-administrator-user-account 
### http://msmvps.com/blogs/erikr/archive/2007/09/26/set-permissions-on-a-specific-service-windows.aspx

function Set-ServiceAcl ($service)
{
	$cmd = "sc sdshow $service"
	$sd = Execute-Cmd $cmd

	Write-Log "Current SD: $sd"
	$sd = $sd.Replace( "S:(", "(A;;RPWPCR;;;AU)S:(" )
	Write-Log "Modified SD: $sd"

	$cmd = "sc sdset $service $sd"
	Execute-Cmd $cmd
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
		$ENV:WINPKG_LOG="$ENV:HADOOP_NODE_INSTALL_ROOT\hadoop-@version@.winpkg.log"
		Write-Log "Logging to $ENV:WINPKG_LOG"
	}

	### $hadoopInstallDir: the directory that contains the appliation, after unzipping
	$hadoopInstallDir = Join-Path "$ENV:HADOOP_NODE_INSTALL_ROOT" "hadoop-@version@"
	$hadoopInstallBin = Join-Path "$hadoopInstallDir" "bin"

	Write-Log "HadoopInstallDir: $hadoopInstallDir"
	Write-Log "HadoopInstallBin: $hadoopInstallBin" 

	Write-Log "Username: $username"
	Write-Log "Password: $password"
	Write-Log "HdfsRole: $hdfsRole"
	Write-Log "MapRedRole: $mapRedRole"
	Write-Log "Ensuring elevated user"

	$currentPrincipal = New-Object Security.Principal.WindowsPrincipal( [Security.Principal.WindowsIdentity]::GetCurrent( ) )
	if ( -not ($currentPrincipal.IsInRole( [Security.Principal.WindowsBuiltInRole]::Administrator ) ) )
	{
		Write-Log "$ENV:WINPKG_LOG FATAL ERROR: install script must be run elevated"
		exit 1
	} 

	if( -not (Test-Path $ENV:JAVA_HOME\bin\java.exe))
	{
		Write-Log "FATAL ERROR: JAVA_HOME not set properly; $ENV:JAVA_HOME\bin\java.exe does not exist"
		exit 1
	}

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

	if( $username -eq $null )
	{
		Write-Log "Invalid command line: -UserName argument is required"
		exit 1
	}

	if( $password -eq $null )
	{
		Write-Log "Invalid command line: -Password is required"
		exit 1
	}

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
		$ENV:ONEBOX_HDFS = "namenode datanode"
	}

	if( -not (Test-Path ENV:ONEBOX_MR))
	{
		$ENV:ONEBOX_MR = "jobtracker tasktracker"
	}

	if( $hdfsRole -eq $null )
	{
		$hdfsRole = "ONEBOX_HDFS"
	}

	if( $mapdredRole -eq $null )
	{
		$mapredRole = "ONEBOX_MR"
	}

	###
	###  Unzip Hadoop distribution from compressed archive
	###

	Write-Log "Extracting Hadoop Core archive into $hadoopInstallDir"
	$unzip_cmd = "$HDP_RESOURCES_DIR\unzip.exe `"$HDP_RESOURCES_DIR\hadoop-@version@.zip`" `"$ENV:HADOOP_NODE_INSTALL_ROOT`""
	Execute-Cmd	$unzip_cmd	

	$xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template`" `"$hadoopInstallDir`""
	Execute-Cmd $xcopy_cmd

	###
	### Grant Hadoop user access to HADOOP_INSTALL_DIR and HDFS Root
	###
	$cmd = "icacls `"$hadoopInstallDir`" /grant ${username}:(OI)(CI)F"
	Execute-Cmd $cmd

	if( -not (Test-Path $ENV:HDFS_DATA_DIR))
	{
		Write-Log "Creating HDFS Data Directory $ENV:HDFS_DATA_DIR"
		mkdir $ENV:HDFS_DATA_DIR
	}

	$cmd = "icacls `"$ENV:HDFS_DATA_DIR`" /grant ${username}:(OI)(CI)F"
	Execute-Cmd $cmd

	###
	### Create Hadoop Windows Services and grant user ACLS to start/stop
	###

	$logonString = "obj=.\$username password=$password"

	$hdfsRoleServices = $executionContext.InvokeCommand.ExpandString( "`$ENV:$hdfsRole" )
	$mapRedRoleServices = $executionContext.InvokeCommand.ExpandString( "`$ENV:$mapredRole" )

	Write-Log "Node HDFS Role Services: $hdfsRoleServices"
	Write-Log "Node MAPRED Role Services: $mapRedRoleServices"

	$allServices = $hdfsRoleServices + " " + $mapRedRoleServices

	Write-Log "Installing services $allServices"

	foreach( $service in $allServices.Split(' '))
	{
		try
		{
			Write-Log "Creating service $service in as $hadoopInstallBin\$service.exe"
			Copy-Item "$HDP_RESOURCES_DIR\serviceHost.exe" "$hadoopInstallBin\$service.exe" -Force

			$cmd="$ENV:WINDIR\system32\sc.exe create $service binPath= `"$hadoopInstallBin\$service.exe`" DisplayName= `"Hadoop $service`" $logonString"
			Execute-Cmd $cmd

			$cmd="$ENV:WINDIR\system32\sc.exe failure $service reset= 30 actions= restart/5000"
			Execute-Cmd $cmd

			$cmd="$ENV:WINDIR\system32\sc.exe config $service start= auto"
			Execute-Cmd $cmd

			Set-ServiceAcl $service

		}
		finally
		{
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
		Execute-Cmd $cmd		
	}

	###
	### Setup MapRed service config
	### 

	foreach( $service in $mapRedRoleServices.Split( ' ' ))
	{
		Write-Log "Creating service config $hadoopInstallBin\$service.xml"
		$cmd = "$hadoopInstallBin\mapred.cmd --service $service > `"$hadoopInstallBin\$service.xml`""
		Execute-Cmd $cmd		
	}

	###
	### Format the namenode
	###

	$cmd="$hadoopInstallBin\hadoop.cmd namenode -format"
	Execute-Cmd $cmd
}
finally
{

}