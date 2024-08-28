package org.itc.com

import org.apache.spark.SparkContext

object task2 {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[1]", "MySparkName")

    val rdd1= sc.textFile("C:\\Users\\Andriy Ocheretyanyy\\1Training\\Spark_with_Scala\\task1Data.txt")
    val tempById = rdd1.map { line =>
      val parts = line.split(",")
      val sensorID = parts(0)
      val temp = parts(2).toDouble

      if (temp > 50){
        (sensorID, 1)
      } else {
        null
      }
    }
      .filter(x => x != null)
      .reduceByKey((x, y) => x + y)

    tempById.collect().foreach {
      case (sensorID, count) =>
        println(s"Sensor $sensorID has $count values over 50")
    }


  }
}
