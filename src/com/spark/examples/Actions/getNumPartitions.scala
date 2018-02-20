package examples.Actions

import org.apache.spark._
import org.apache.spark.SparkContext._

object getNumPartitions {
	def main(args:Array[String]) {

		    val sc = new SparkContext("local[*]", "getNumPartitions")
				val x = sc.parallelize(Array(1,2,3) , 2)
				val y = x.partitions.size
				println(y)
	}
}