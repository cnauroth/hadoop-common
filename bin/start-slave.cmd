@echo off
setlocal

:StartHadoop
start "Apache Hadoop Distribution" hadoop datanode
start "Apache Hadoop Distribution" hadoop tasktracker
