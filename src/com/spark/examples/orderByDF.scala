package examples

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object orderByDF {
	def main(args:Array[String]) {
		    val spark = SparkSession
				.builder
				.appName("SparkSQL")
				.master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///C:/temp")
				.getOrCreate()

				val input  = Seq(("a",1), ("b",2), ("c",1), ("d",3))
				val df = spark.sqlContext.createDataFrame(input)
				val newNames = Seq("A", "B")
				val dfRenamed = df.toDF(newNames: _*)
        val result = dfRenamed.orderBy(desc("A"))
				println(result.collect().mkString(","))
	}
}