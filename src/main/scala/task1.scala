package org.itc.com

import org.apache.spark.SparkContext

object task1 {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[1]", "MySparkName")

    val rdd1= sc.textFile("C:\\Users\\Andriy Ocheretyanyy\\1Training\\Spark_with_Scala\\task1Data.txt")
    val temp = rdd1.map { line =>
      val parts = line.split(",")
      parts(2).toDouble
    }

    val maxtemp = temp.max()

    println(s"The highest temperature is $maxtemp")

  }
}
