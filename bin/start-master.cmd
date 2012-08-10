@echo off
setlocal

:StartHadoop
start "Apache Hadoop Distribution" hadoop namenode
start "Apache Hadoop Distribution" hadoop jobtracker
