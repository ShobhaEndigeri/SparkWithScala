package stackOverFlow

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object collect_list_DF {
	def main(args:Array[String]) {
		    val spark = SparkSession
				.builder
				.appName("SparkSQL")
				.master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///C:/temp")
				.getOrCreate()

				val input  = Seq((1, 2, 1234), (1, 2, 456))
				val df = spark.sqlContext.createDataFrame(input)
				val newNames = Seq("A", "B","C")
				val dfRenamed = df.toDF(newNames: _*)
        
				val result = dfRenamed.groupBy("A","B").agg(sort_array(collect_list("C")))
				println(result.collect().mkString(","))
				
				dfRenamed.createOrReplaceTempView("my_table")
				spark.sqlContext.sql("SELECT A, B, sort_array(collect_set(C)) AS collected FROM my_table GROUP BY A, B").show()
	}
}