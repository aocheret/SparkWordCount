package org.itc.com

import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming extends App{

  val sc = new SparkContext("local[*]", "streamDemo")
  val strc = new StreamingContext(sc, Seconds(2))
  val lines = strc.socketTextStream("localhost", 9996)
  val wordCount = lines.flatMap(x => x.split("")).map(x=>(x,1)).reduceByKey(((x,y) => (x+y)))
  wordCount.print()

  strc.start()

  strc.awaitTermination()

// ncat -lvp 9996 in C:\Program Files (x86)\nmap
}
