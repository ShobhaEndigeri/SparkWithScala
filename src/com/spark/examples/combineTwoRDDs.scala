package examples

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object combineTwoRDDs {
	def main(args:Array[String]) {
		val sc = new SparkContext("local[*]", "combineTwoRDDs")
				val rdd1 = sc.parallelize(Array(('a', 10),('b', 15),('c', 20),('d', 20),('e', 13)))
				val rdd2 = sc.parallelize(Array((1,'a'),(2,'b'),(3,'e')))

				val result = rdd1.join(rdd2.map(_.swap)).values
				println(result.collect().mkString(","))
	}
}