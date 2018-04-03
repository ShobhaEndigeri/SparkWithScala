package stackOverFlow

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object groupByDF {
	def main(args:Array[String]) {
		    val spark = SparkSession
				.builder
				.appName("SparkSQL")
				.master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///C:/temp")
				.getOrCreate()

				val input  = Seq(("a",15), ("b",2), ("a",11), ("d",3),("a",10))
				val df = spark.sqlContext.createDataFrame(input)
				val newNames = Seq("A", "B")
				val dfRenamed = df.toDF(newNames: _*)
        val result = dfRenamed.groupBy("A").agg(sort_array(collect_list("B"))).sort(asc("A"))
				println(result.collect().mkString(","))
	}
}