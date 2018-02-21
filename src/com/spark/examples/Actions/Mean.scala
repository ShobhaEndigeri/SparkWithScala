package examples.Actions

import org.apache.spark._
import org.apache.spark.SparkContext._

object Mean {
	def main(args:Array[String]) {
		val sc = new SparkContext("local[*]", "Mean")
		val x = sc.parallelize(Array(1,2,3,4))
		
		val y = x.mean()
    println(y)
	}
}