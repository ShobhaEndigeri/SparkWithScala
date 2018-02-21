package examples.sql

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._


object LoadJson {
  def main(args: Array[String]) {
    	val spark = SparkSession
			.builder
			.appName("SparkSQL")
			.master("local[*]")
			.config("spark.sql.warehouse.dir", "file:///C:/temp")
			.getOrCreate()

			
    val inputFile = args(0)
    val input = spark.sqlContext.jsonFile(inputFile)
    input.printSchema()
  }
}