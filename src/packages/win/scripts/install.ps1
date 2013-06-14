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

###
### Install script that can be used to install Hadoop as a Single-Node cluster.
### To invoke the scipt, run the following command from PowerShell:
###   install.ps1 -username <username> -password <password>
###
### where:
###   <username> and <password> represent account credentials used to run
###   Hadoop services as Windows services.
###
### Account must have the following two privileges, otherwise
### installation/runtime will fail.
###   SeServiceLogonRight
###   SeCreateSymbolicLinkPrivilege
###
### By default, Hadoop is installed to "C:\Hadoop". To change this set
### HADOOP_NODE_INSTALL_ROOT environment variable to a location were
### you'd like Hadoop installed.
###
### Script pre-requisites:
###   JAVA_HOME must be set to point to a valid Java location.
###
### To uninstall previously installed Single-Node cluster run:
###   uninstall.ps1
###
### NOTE: Notice @version@ strings throughout the file. First compile
### winpkg with "ant winpkg", that will replace the version string.
### To install, use:
###   build\hadoop-@version@.winpkg.zip#scripts\install.ps1
###

param(
    [String]
    [Parameter( ParameterSetName='UsernamePassword', Position=0, Mandatory=$true )]
    [Parameter( ParameterSetName='UsernamePasswordBase64', Position=0, Mandatory=$true )]
    $username,
    [String]
    [Parameter( ParameterSetName='UsernamePassword', Position=1, Mandatory=$true )]
    $password,
    [String]
    [Parameter( ParameterSetName='UsernamePasswordBase64', Position=1, Mandatory=$true )]
    $passwordBase64,
    [Parameter( ParameterSetName='CredentialFilePath', Mandatory=$true )]
    $credentialFilePath,
    [String]
    $hadooproles="NAMENODE SECONDARYNAMENODE JOBTRACKER SLAVE",
    [Switch]
    $skipNamenodeFormat = $false
    )

function Main( $scriptDir )
{
    if ( -not (Test-Path ENV:WINPKG_LOG))
    {
        $ENV:WINPKG_LOG = "hadoop.core.winpkg.log"
    }

    $HadoopCoreVersion = "@version@"
    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "hadoop-$hadoopCoreVersion.winpkg.log"
    Test-JavaHome

    ### $hadoopInstallDir: the directory that contains the appliation, after unzipping
    $nodeInstallRoot = "$ENV:HADOOP_NODE_INSTALL_ROOT"
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"
    $hadoopInstallToBin = Join-Path "$hadoopInstallToDir" "bin"
    
    Write-Log "nodeInstallRoot: $nodeInstallRoot"
    Write-Log "hadoopInstallToBin: $hadoopInstallToBin"

    ###
    ### Create the Credential object from the given username and password or the provided credentials file
    ###
    $serviceCredential = Get-HadoopUserCredentials -credentialsHash @{"username" = $username; "password" = $password; `
        "passwordBase64" = $passwordBase64; "credentialFilePath" = $credentialFilePath}
    $username = $serviceCredential.UserName
    Write-Log "Username: $username"
    Write-Log "CredentialFilePath: $credentialFilePath"
    
    ###
    ### Initialize root directory used for Core, HDFS and MapRed local folders
    ###
    if( -not (Test-Path ENV:HDFS_DATA_DIR))
    {
        $ENV:HDFS_DATA_DIR = Join-Path "$ENV:HADOOP_NODE_INSTALL_ROOT" "hdfs"
    }

    ###
    ### Stop all services before proceeding with the install step, otherwise
    ### files will be in-use and installation can fail
    ###
    Write-Log "Stopping Hadoop services if already running before proceeding with install"
    StopService "historyserver tasktracker jobtracker datanode secondarynamenode namenode"


    ###
    ### Install and Configure Core
    ###
    # strip out domain/machinename if it exists. will not work with domain users.
    $shortUsername = $username
    if($username.IndexOf('\') -ge 0)
    {
        $shortUsername = $username.SubString($username.IndexOf('\') + 1)
    }

    InstallCore $NodeInstallRoot $serviceCredential

    ###
    ### Configure Core HDFS Mapred
    ###    
    Configure "core" $NodeInstallRoot $serviceCredential @{
        "hadoop.tmp.dir" = Join-Path (${ENV:HDFS_DATA_DIR}.Split(",") | Select -first 1).Trim() "tmp";
        "fs.checkpoint.dir" = Get-AppendedPath $ENV:HDFS_DATA_DIR "snn";
        "fs.checkpoint.edits.dir" = Get-AppendedPath $ENV:HDFS_DATA_DIR "snn";
        "hadoop.proxyuser.$shortUsername.groups" = "HadoopUsers";
        "hadoop.proxyuser.$shortUsername.hosts" = "*" }

    ###
    ### Configure HDFS
    ###
    Configure "hdfs" $NodeInstallRoot $serviceCredential @{
        "dfs.name.dir" = Get-AppendedPath $ENV:HDFS_DATA_DIR "nn";
        "dfs.data.dir" = Get-AppendedPath $ENV:HDFS_DATA_DIR "dn" }

    ###
    ### Configure MapRed
    ###
    Configure "mapreduce" $NodeInstallRoot $serviceCredential @{
        "mapred.local.dir" = Get-AppendedPath $ENV:HDFS_DATA_DIR "mapred\local";
        "mapred.child.tmp" = Join-Path (${ENV:HDFS_DATA_DIR}.Split(",") | Select -first 1).Trim() "tmp" }

    ###
    ### Check the nn, dn, snn and mapred directories
    ###
    $skipNamenodeFormat = (CheckDataDirectories $hadoopInstallToDir)

    Write-Log "Install of Hadoop Core, HDFS, MapRed completed successfully"

    ###
    ### Install Hadoop services
    ###    
    foreach ( $role in $hadooproles.split(" ") ) {
        if ((iex `$'ENV:IS_'$role) -eq ("yes"))
        {
            if ($role -eq "JOBTRACKER" ) {
                $roles_to_start = $roles_to_start+" "+"jobtracker"+" "+"historyserver"
            }
            elseif ($role -eq "SLAVE" ) {
                $roles_to_start = $roles_to_start+" "+"datanode"+" "+"tasktracker"
            }
            else {
                $roles_to_start = $roles_to_start+" "+$role.ToLower()
            }
        }
    }
    InstallService $NodeInstallRoot $serviceCredential "$roles_to_start"  
    if ( ($skipNamenodeFormat -ne $true) -and ($ENV:IS_NAMENODE -eq "yes") )
      {
	    ###
	    ### Format the namenode
	    ###
	    FormatNamenode $false
      }
      else
      {
	    Write-Log "Skipping Namenode format"
      }
   }

try
{
    $scriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)
    $utilsModule = Import-Module -Name "$scriptDir\..\resources\Winpkg.Utils.psm1" -ArgumentList ("HADOOP") -PassThru
    $apiModule = Import-Module -Name "$scriptDir\InstallApi.psm1" -PassThru
    Main $scriptDir
}
catch
{
	Write-Log $_.Exception.Message "Failure" $_
	exit 1
}
finally
{
    if( $apiModule -ne $null )
    {
        Remove-Module $apiModule
    }
    if( $utilsModule -ne $null )
    {
        Remove-Module $utilsModule
    }
}
