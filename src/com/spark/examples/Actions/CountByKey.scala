package examples.Actions

import org.apache.spark._
import org.apache.spark.SparkContext._

object CountByKey {
	def main(args:Array[String]) {
		val sc = new SparkContext("local[*]", "CountByKey")
		val x = sc.parallelize(Array(('J',"James"),('F',"Fred"),('A',"Anna"),('J',"John")))
		
		val y = x.countByKey()
    println(y)
	}
}