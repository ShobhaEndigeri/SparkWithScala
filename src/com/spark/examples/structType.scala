package examples

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object structType {
	def main(args:Array[String]) {
		val spark = SparkSession
				.builder
				.appName("SparkSQL")
				.master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///C:/temp")
				.getOrCreate()

				import spark.implicits._
				import org.apache.spark.sql.types.StructType

				val schemaUntyped = new StructType()
				.add("a", "int")
				.add("b", "string")

				val schemaUntyped_2 = new StructType()
				.add($"a".int)
				.add($"b".string)

	}
}