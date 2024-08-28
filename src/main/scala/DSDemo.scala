package org.itc.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lower, rank, trim, when, window}
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructField, StructType}


object DSDemo extends App {

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

  val prodDf = spark.read.option("header", true).schema(prodSchema).csv("C:\\Users\\Andriy Ocheretyanyy\\Downloads\\warehouse\\warehouse\\products_cleaned\\part-00000-e880ce23-fb28-4c8f-84d7-7f882da89123-c000.csv")

  case class prodData(product_number:String,
                      product_name:String,
                      product_category:String,
                      product_scale:String,
                      product_manufacturer:String,
                      product_description:String,
                      length:Double,
                      width:Double,
                      height:Float)

  import spark.implicits._
  val productDs = prodDf.as[prodData]

  productDs.show(5)


}