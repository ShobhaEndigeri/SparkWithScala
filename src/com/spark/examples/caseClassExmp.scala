package examples

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object caseClassExmp {
  case class Person (name:String,id:Int)
  
  	def main(args:Array[String]) {
		    val spark = SparkSession
				.builder
				.appName("SparkSQL")
				.master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///C:/temp")
				.getOrCreate()
				
				var input  = Array(("Shobha",1), ("Sonu",2), ("Shreya",3), ("Naveen",4))
				var personRDD = input.map(p => Person(p._1,p._2.toInt))
				val personDF = spark.createDataFrame(personRDD)
				personDF.registerTempTable ("person")
				val people = spark.sql("select * from person")
				people.collect.foreach (println)
  	}
}