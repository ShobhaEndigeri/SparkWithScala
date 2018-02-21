package examples.Actions

import org.apache.spark._
import org.apache.spark.SparkContext._

object Reduce {
	def main(args:Array[String]) {
		val sc = new SparkContext("local[*]", "Reduce")
		val x = sc.parallelize(Array(1,2,3,4))
		
		val y = x.reduce((a,b) => (a+b))
		println(x.collect.mkString(", "))
    println(y)
    
	}
}