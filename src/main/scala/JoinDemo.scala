package org.itc.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, col, lower, rank, trim, when, window}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
import org.itc.com.SQLDemo.{prodDf, spark}


object JoinDemo extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "DataFrameDemo")
  sparkconf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkconf).getOrCreate()

  val prodSchema = StructType(Array(
    StructField("product_number", StringType, nullable = false),
    StructField("product_name", StringType, nullable = false),
    StructField("product_category", StringType, nullable = false),
    StructField("product_scale", StringType, nullable = false),
    StructField("product_manufacturer", StringType, nullable = false),
    StructField("product_description", StringType, nullable = false),
    StructField("length", FloatType, nullable = false),
    StructField("width", FloatType, nullable = false),
    StructField("height", FloatType, nullable = false)
  )) //better way

  val prodDf = spark.read.option("header", true).schema(prodSchema).csv("C:\\Users\\Andriy Ocheretyanyy\\Downloads\\warehouse\\warehouse\\products.csv")


  val orderSchema = StructType(Array(
    StructField("order_number", StringType, nullable = false),
    StructField("product_number", StringType, nullable = false),
    StructField("product_category", StringType, nullable = false),
    StructField("price", IntegerType, nullable = false),
    StructField("quantity", IntegerType, nullable = false)
  ))

  val orderDf = spark.read.option("header", true).schema(orderSchema).csv("C:\\Users\\Andriy Ocheretyanyy\\Downloads\\warehouse\\warehouse\\orderdetails.csv")

  val jj = prodDf.join(orderDf,prodDf.col("product_number")===orderDf.col("product_number"))
  jj.show(5)

  prodDf.createOrReplaceTempView("prod")
  orderDf.createOrReplaceTempView("order")

  spark.sql(
    """select * from prod p
      |join order o on p.product_number = o.product_number
      |""".stripMargin).show(5)

  val jj2 = orderDf.join(broadcast(prodDf),prodDf.col("product_number")===orderDf.col("product_number"))
  jj2.show(5)



}