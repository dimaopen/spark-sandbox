#!/usr/bin/env bash

GC_FLAGS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -Dorg.bytedeco.javacpp.maxretries=100 -Dorg.bytedeco.javacpp.maxbytes=2500000000"
BIGTOP_JAVA_MAJOR=8 # ensures Java 8 on distros like CDH
/Users/Dmitry/Developer/spark-2.2.1-bin-hadoop2.7/bin/spark-submit \
--master yarn \
--deploy-mode client \
--conf spark.driver.extraJavaOptions="$GC_FLAGS" \
--conf spark.executor.extraJavaOptions="$GC_FLAGS" \
--conf spark.locality.wait=0 \
--conf spark.driver.maxResultSize=2g \
--conf spark.yarn.executor.memoryOverhead=2g \
--conf spark.yarn.driver.memoryOverhead=2g \
--conf spark.executor.cores=1 \
--conf spark.cores.max=1 \
--verbose \
--class "dopenkov.bigdata.SparkWinner" \
./target/scala-2.11/spark-sandbox-assembly-0.0.1.jar \
hdfs://localhost:9000/user/Dmitry/deutsche-boerse-xetra-pds/ \
hdfs://localhost:9000/spark-result