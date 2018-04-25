package dopenkov.bigdata

import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{max, _}
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
  *
  * @author Dmitry Openkov
  */
object SparkWinner extends App {
  private val logger = Logger.getLogger("mySparkJobLogger")
  val in = args(0)
  val out = args(1)
  println(s"in, out = $in -> $out")

  val spark = org.apache.spark.sql.SparkSession.builder
    .getOrCreate

  import spark.implicits._

  val lines: Dataset[Row] = spark.read
    .option("header", value = true)
    .csv(s"$in/*/*.csv")
    .withColumn("securityID", col("SecurityID").cast(LongType))
    .withColumn("startPrice", col("StartPrice").cast(DoubleType))
    .withColumn("endPrice", col("EndPrice").cast(DoubleType))
    .drop("ISIN", "Mnemonic", "SecurityType", "Currency", "MaxPrice", "MinPrice", "TradedVolume", "NumberOfTrades")

  val wMin = Window.partitionBy("securityID", "date").orderBy($"time".asc)
  val wMax = Window.partitionBy("securityID", "date").orderBy($"time".desc)
  val minTimeDS = lines.withColumn("rn", row_number.over(wMin)).where($"rn" === 1).drop("rn").drop("EndPrice")
  val maxTimeDS = lines.withColumn("rn", row_number.over(wMax)).where($"rn" === 1).drop("rn").drop("StartPrice")

  val promVal: DataFrame = minTimeDS.join(maxTimeDS, Seq("securityID", "date")).drop(maxTimeDS("SecurityDesc"))
    .withColumn("percentage", expr("(EndPrice - StartPrice) / StartPrice as percentage"))
    .drop("time", "StartPrice", "EndPrice")

  val result: DataFrame = promVal.groupBy("date").agg(max("percentage").as("percentage"))
    .join(promVal, Seq("date", "percentage"), "left").repartition(1).orderBy("date")
  result.rdd.saveAsTextFile(out)
}
