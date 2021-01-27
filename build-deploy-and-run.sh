#!/bin/bash
app=my-apache-spark-java-0.1-SNAPSHOT
mvn clean package -Pmake-dist && \
scp target/$app-bin.tar.gz dev1: && \
ssh dev1 "rm -rf $app && tar xzf $app-bin.tar.gz && cd $app/bin && ./run.sh"
