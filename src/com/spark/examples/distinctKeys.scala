package stackOverFlow

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object distinctKeys {
	def main(args:Array[String]) {
		val spark = SparkSession
				.builder
				.appName("SparkSQL")
				.master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///C:/temp")
				.getOrCreate()

				import spark.implicits._

				val rdd1 = spark.sparkContext.parallelize(Array(("key1","value1"),("key2","value2"),("key2","value3"),("key2","value4")))
				//.groupBy("k")
				//.agg(collect_set(col("v")))
				//result.foreach(x=>println(x))

				val newNames = Seq("id", "value")
				val df = spark.sqlContext.createDataFrame(rdd1)//.agg(collect_list("ddd"))
				val dfRenamed = df.toDF(newNames: _*)
				dfRenamed.agg(collect_list("value"))
				
				dfRenamed.createOrReplaceTempView("my_table")
				spark.sqlContext.sql("SELECT * FROM my_table").show()
				
				//https://stackoverflow.com/questions/45999622/spark-scala-merge-mutiple-rows-into-one?noredirect=1&lq=1

	}
}