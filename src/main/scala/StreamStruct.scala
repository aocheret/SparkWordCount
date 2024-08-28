package org.itc.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamStruct extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "DFDemo")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  val lines = spark.readStream.format("socket").option("host", "localhost").option("port",9996).load()


  import spark.implicits._

  val words = lines.as[String].flatMap(_.split(" "))
  val wordCount = words.groupBy("value").count

  val prod = wordCount.writeStream.outputMode("complete").format("console")
    .trigger(Trigger.ProcessingTime("1 second"))
    .option("checkpointLocation", "C:/Users/ANDRIY~1/1Training/Spark_with_Scala/Spark/checkpoint-location")
    .start()

  prod.awaitTermination()

}
