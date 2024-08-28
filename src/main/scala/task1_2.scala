package org.itc.com

import org.apache.spark.SparkContext

object task1_2 {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[1]", "MySparkName")

    val rdd1= sc.textFile("C:\\Users\\Andriy Ocheretyanyy\\1Training\\Spark_with_Scala\\task1Data.txt")
    val tempID = rdd1.map { line =>
      val parts = line.split(",")
      val sensorID = parts(0)
      val temp = parts(2).toDouble
      (sensorID, temp)
    }.reduceByKey(Math.max)

    val maxtemp = tempID.map(_._2).max()

    println(s"The highest temperature is $maxtemp")

  }
}
