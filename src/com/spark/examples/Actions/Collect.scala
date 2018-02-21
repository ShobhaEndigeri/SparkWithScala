package examples.Actions

import org.apache.spark._
import org.apache.spark.SparkContext._

object Collect {
	def main(args:Array[String]) {
		val sc = new SparkContext("local[*]", "Collect")
		val x= sc.parallelize(Array(1,2,3), 2)
		val y= x.collect()
	}
}