package examples

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object substractByKey {
	def main(args:Array[String]) {
		    val sc = new SparkContext("local[*]", "substractByKey")
			val rdd1 = sc.parallelize(Array(('a', 10),('b', 15),('c', 20),('d', 20),('e', 13)))
			val rdd2 = sc.parallelize(Array(('a', 11),('b', 12),('c', 22)))

			rdd1Grouped.subtractByKey(rdd2Grouped)*/
			val result = rdd1.subtractByKey(rdd2);
		    println(result.collect().mkString(","))
	}

}