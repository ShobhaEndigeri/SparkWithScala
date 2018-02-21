package examples.Actions

import org.apache.spark._
import org.apache.spark.SparkContext._

object Max {
	def main(args:Array[String]) {
		val sc = new SparkContext("local[*]", "Max")
		val x = sc.parallelize(Array(1,2,3,4))
		
		val y = x.max();
    println(y)
	}
}