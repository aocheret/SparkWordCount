package org.itc.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.streaming.{Seconds, StreamingContext}

object fileStream extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "DFDemo")
  sparkConf.set("spark.master", "local[*]")
  sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
  sparkConf.set("spark.sql.streaming.schemaInference", "true")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val ddlSchema =
    """order_id int, order)date string, order_customer_id int, order_status, amount int""".stripMargin

  val ordersDF = spark.readStream.format("json").option("path", "input").load()

  ordersDF.createOrReplaceTempView("order")

  val onhold = spark.sql("select * from order where order_status = 'ON_HOLD'")

  val res = onhold.writeStream.format("json").option("path", "output").outputMode("append")
    .option("checkpointLocation", "C:/Users/ANDRIY~1/1Training/Spark_with_Scala/Spark/checkpointLoc")
    .trigger(Trigger.ProcessingTime("20 seconds"))
    .start()

  res.awaitTermination()

}
