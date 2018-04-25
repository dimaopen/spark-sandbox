package dopenkov.bigdata

import java.lang

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.log4j.Logger

import scala.collection.JavaConversions._

/**
  *
  * @author Dmitry Openkov
  */
class BiggestWinnerMapper extends Mapper[LongWritable, Text, Text, Text] {
  val parserSettings: CsvParserSettings = new CsvParserSettings
  parserSettings.getFormat.setLineSeparator("\n")
  val parser: CsvParser = new CsvParser(parserSettings)
  val dateKey = new Text()
  val outValue = new Text()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    if (key.get() > 0) {
      val values: Array[String] = parser.parseLine(value.toString)
      val securityId = values(5)
      val securityDesc = values(2)
      val date = values(6)
      val startPrice = values(8)
      val endPrice = values(11)
      val startTime = values(7)
      dateKey.set(date + "|" + securityId)
      outValue.set(securityDesc + "|" + startTime + "|" + startPrice + "|" + endPrice)
      context.write(dateKey, outValue)
    }
  }
}

class BiggestWinnerReducer extends Reducer[Text, Text, Text, Text] {
  private val outValue = new Text()
  private val logger = Logger.getLogger("myJobLogger")

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val (start, end) = values.toIterable.map(_.toString.split('|'))
      .foldLeft((Array("N/A", "24:00", "0", "0"), Array("N/A", "00:00", "0", "0"))) { case ((min, max), e) => (if (min(1).compareTo(e(1)) < 0) min else e, if (max(1).compareTo(e(1)) > 0) max else e) }
    val startPrice: Float = start(2).toFloat
    val endPrice: Float = end(3).toFloat
    logger.info(s"key = $key start = ${start.toList} end = ${end.toList}")
    println(s"println msg1 key = $key start = ${start.toList} end = ${end.toList}")
    val percentage = (endPrice - startPrice) / startPrice
    outValue.set(start(0) + "|" + percentage)
    context.write(key, outValue)
  }
}

class BiggestWinnerMapper2 extends Mapper[LongWritable, Text, Text, Text] {
  val pattern = "([0-9\\-]+)\\|(\\d+)\\s+([^|]+)\\|(.+)".r
  val dateKey = new Text()
  val outValue = new Text()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    value.toString match {
      case pattern(date, securityId, securityDesc, percentage) =>
        dateKey.set(date)
        outValue.set(securityId + "|" + securityDesc + "|" + percentage)
        context.write(dateKey, outValue)
      case _ => println(s"cannot parse ${value.toString}")
    }
  }
}

class BiggestWinnerReducer2 extends Reducer[Text, Text, Text, Text] {
  val outValue = new Text()

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val winner = values.toIterable.map(_.toString.split('|')).map(ar => (ar(0), ar(1), ar(2).toFloat)).maxBy(_._3)
    outValue.set(winner._1 + '\t' + winner._2 + '\t' + winner._3)
    context.write(key, outValue)
  }
}

object BiggestWinner extends App {
  private val logger = Logger.getLogger("BiggestWinnerMain")
  logger.info("BiggestWinner starting")
  println("BiggestWinnerStarting")
  val conf = new Configuration()
  conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

  val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
  println(s"otherArgs = ${otherArgs.toList}")
  val goodArg = (otherArgs.length == 2 && !otherArgs(0).contains("BiggestWinner")
    || otherArgs.length == 3 && otherArgs(0).contains("BiggestWinner"))
  if (!goodArg) {
    println("Usage: BiggestWinner <in> <out>")
    2
  } else {
    import org.apache.hadoop.fs.FileSystem

    val inter1 = new Path("./intermediate1")
    val outPath = new Path(getOut)
    val fs = FileSystem.get(conf)
    fs.delete(inter1, true)

    val job1: Job = createJob("find percentage", classOf[BiggestWinnerMapper], classOf[BiggestWinnerReducer],
      new Path(getIn), inter1)
    val job2 = createJob("find max percentage", classOf[BiggestWinnerMapper2], classOf[BiggestWinnerReducer2],
      inter1, outPath)

    val result1 = job1.waitForCompletion(true)
    val result2 = result1 && job2.waitForCompletion(true)
    (if (result2) 0 else 2) + (if (result1) 0 else 1)
  }

  private def getOut = {
    if (otherArgs.length == 3) otherArgs(2) else otherArgs(1)
  }

  private def getIn = {
    if (otherArgs.length == 3) otherArgs(1) else otherArgs(0)
  }

  private def createJob(name: String, mapper: Class[_ <: Mapper[_, _, _, _]], reducer: Class[_ <: Reducer[_, _, _, _]],
                        in: Path, out: Path): Job = {
    val job = Job.getInstance(conf, name)
    job.setJarByClass(classOf[BiggestWinnerMapper])
    job.setMapperClass(mapper)
    job.setReducerClass(reducer)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job, in)
    FileOutputFormat.setOutputPath(job, out)
    job
  }
}