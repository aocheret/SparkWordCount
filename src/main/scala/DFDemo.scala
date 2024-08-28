package org.itc.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lower, trim}
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructField, StructType}

object DFDemo extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name","DataFrameDemo")
  sparkconf.set("spark.master","local[*]")

  val spark = SparkSession.builder().config(sparkconf).getOrCreate()

  val prodSchema = StructType(Array(
    StructField("product_number",StringType,nullable=false),
    StructField("product_name",StringType,nullable=false),
    StructField("product_category",StringType,nullable=false),
    StructField("product_scale",StringType,nullable=false),
    StructField("product_manufacturer",StringType,nullable=false),
    StructField("product_description",StringType,nullable=false),
    StructField("length",DoubleType,nullable=false),
    StructField("width",DoubleType,nullable=false),
    StructField("height",FloatType,nullable=false)
  )) //better way

  val ddlSchema =
    """product_number string, product_name string, product_category string, product_scale string, product_manufacturer string, product_description string,
      |length double, width double, height float""".stripMargin
  val prodDf = spark.read.option("header",true).schema(ddlSchema).csv("C:\\Users\\Andriy Ocheretyanyy\\Downloads\\warehouse\\warehouse\\products.csv")

  var castDf = prodDf.withColumn("length", col("length").cast("FLoat"))

  castDf = castDf.dropDuplicates("product_number")
  castDf = castDf.na.fill("unknown",Seq("product_name")).na.fill(0,Seq("length","width","height"))
  castDf = castDf.withColumn("product_number",trim(col("product_number")))
  castDf = castDf.withColumn("product_category",lower(col("product_category")))
    .withColumn("product_name",lower(col("product_name")))

  castDf = castDf.filter(col("width")>85)

  castDf.describe("length","width","height").show()
  castDf.coalesce(1).write.csv("C:\\Users\\Andriy Ocheretyanyy\\Downloads\\warehouse\\warehouse\\products_cleaned")
//  castDf.printSchema()
//  castDf.show(5)

}
