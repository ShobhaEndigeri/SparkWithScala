package examples.Actions

import org.apache.spark._
import org.apache.spark.SparkContext._

object Sum {
	def main(args:Array[String]) {
		val sc = new SparkContext("local[*]", "Sum")
		val x = sc.parallelize(Array(1,2,3,4))
		
		val y = x.sum()
    println(y)
	}
}