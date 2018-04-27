package examples

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object filterRDD {
	def main(args:Array[String]) {
		val spark = SparkSession
				.builder
				.appName("SparkSQL")
				.master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///C:/temp")
				.getOrCreate()

				val rdd = spark.sparkContext.textFile("input.csv")
				val filteredRDD = rdd.filter(line => line.split(",")(0).equals("abcd"))
				filteredRDD.first.foreach(print)
	}
}