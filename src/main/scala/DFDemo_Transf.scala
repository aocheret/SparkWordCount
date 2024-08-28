package org.itc.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lower, rank, trim, when, window}
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructField, StructType}

object DFDemo_Transf extends App{

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
    StructField("length",FloatType,nullable=false),
    StructField("width",FloatType,nullable=false),
    StructField("height",FloatType,nullable=false)
  )) //better way

  val prodDf = spark.read.option("header",true).schema(prodSchema).csv("C:\\Users\\Andriy Ocheretyanyy\\Downloads\\warehouse\\warehouse\\products_cleaned\\part-00000-e880ce23-fb28-4c8f-84d7-7f882da89123-c000.csv")

  var selectedDf = prodDf.select("product_number", "product_name", "product_description", "length", "product_category")
    .filter(col("length") > 100)
    .orderBy("product_description")

  selectedDf = selectedDf.withColumn("product_size", when(col("length")<4000,"Small")
    .when(col("length")<6000,"Medium")
    .when(col("length")<8000,"Large")
    .otherwise("Extra Large")
  )
//window func rank based on length for each category
  val windspec = Window.partitionBy("product_category").orderBy(col("length").desc)

  val rankDf = selectedDf.withColumn("Ranking", rank().over(windspec))

//  pivot
  val pivotDf = rankDf.groupBy("product_name").pivot("product_category").agg(functions.min(col("length")))

  pivotDf.show()

  //find outlier for l,w,h
//  add 2 new columns based on prod_num, split by _ and first part - store_name, second - prod_num
//  add new col prod_class considering l and w
  //  if l > 4000 and w > 20 = small and wide
  //  if l > 6000 and w > 40 = small and narrow
  //  if l > 8000 and w > 60 = large and wide
//  else large and narrow
//  find min, max, avg of l for each category

}
