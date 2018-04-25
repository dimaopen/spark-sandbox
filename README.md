# Hadoop, Spark samples in Scala

A sample app that processes some of open [Deutsche Börse Public Dataset (DBG PDS)](https://github.com/Deutsche-Boerse/dbg-pds).

It does the same job as [this example script](https://github.com/Deutsche-Boerse/dbg-pds/blob/master/examples/sql/xetra_biggest_winner.sql) does.

## Building and uploading to AWS

1. Execute the following commands
  ```shell
sbt assembly
aws s3 cp target/scala-2.11/spark-sandbox-assembly-0.0.1.jar s3://deutsche-boerse/bin/assembly.jar
  ```
You need to replace deutsche-boerse with your aws bucket name.

## How to run

### Hadoop job
1. Start your EMR cluster that includes Hadoop and Spark environments.
1. Add a custom jar step to your cluster with the following parameters

* *JAR location*: s3://deutsche-boerse/bin/assembly.jar
* *Main class*: None
* *Arguments*: dopenkov.bigdata.BiggestWinner s3://deutsche-boerse/xetra-pds s3://deutsche-boerse/xetra-winner
* *Action on failure*: Cancel and wait

After the step is completed you can view the results at s3://deutsche-boerse/xetra-winner.

### Spark job
1. Start your EMR cluster that includes Hadoop and Spark environments.
1. Add a Spark-submit step to your cluster with the following parameters

--deploy-mode cluster --master yarn --verbose --class dopenkov.bigdata.SparkWinner s3://deutsche-boerse/bin/assembly.jar s3://deutsche-boerse/xetra-pds s3://deutsche-boerse/spark-winner

After the step is completed you can view the results at s3://deutsche-boerse/spark-winner.