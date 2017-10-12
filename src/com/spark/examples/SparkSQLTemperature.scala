package com.spark.examples

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQLTemperature {
case class Temperarure(year:Int,temp:Int)

def mapper(line:String):Temperarure = {
		val fields = line.split(',')  
	  val temp:Temperarure = Temperarure(fields(0).toInt,fields(1).toInt)
		temp
}

def main(args:Array[String]) {
	Logger.getLogger("org").setLevel(Level.ERROR)

	val spark = SparkSession
	.builder
	.appName("SparkSQL")
	.master("local[*]")
	.config("spark.sql.warehouse.dir", "file:///C:/temp")
	.getOrCreate()

	val lines = spark.sparkContext.textFile("../temperarureFile.txt")
	val temp = lines.map(mapper)

	import spark.implicits._
	val schemaTemp = temp.toDS

	schemaTemp.printSchema()

	schemaTemp.createOrReplaceTempView("temperature")

	val temps = spark.sql("SELECT * FROM temperature WHERE temp > 50")

	val results = temps.collect()

	results.foreach(println)

	spark.stop()
}
}
