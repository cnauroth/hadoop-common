@echo off
setlocal
set PATH=%PATH%;%HADOOP_BIN_PATH%

:StartHadoop
start "Apache Hadoop Distribution" hadoop namenode
start "Apache Hadoop Distribution" hadoop jobtracker
