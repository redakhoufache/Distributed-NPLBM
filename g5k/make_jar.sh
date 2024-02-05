#!/bin/bash

export PATH="$PATH:/opt/bitnami/spark/apache-maven-3.9.1/bin"
cd Distributed-NPLBM
mvn package -DskipTests 
