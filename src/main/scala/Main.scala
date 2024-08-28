package org.itc.com
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {

//    val conf = new SparkConf().setAppName("TestApp")
//    val sc = new SparkContext(conf)
//    val data = sc.parallelize(Seq("hello", "world"))
//    data.foreach(println)

    println("Hello world!")

    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Hello world!2")
    val scf = new SparkConf()
    scf.setAppName("Demo")
    println("Hello world!3")
    val sc = new SparkContext(scf)
    println("Hello world!4")
    val rdd1= sc.textFile(args(0))
    val words = rdd1.flatMap(line => line.split(" "))

    val wordcount = words.map(w => (w.toLowerCase(),1))

    val res = wordcount.reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey()

    res.collect().foreach(println)

    //    count by key
//    val res1 = wordcount.countByKey()
//
//    res1.foreach(println)
    sc.stop()

  }
}