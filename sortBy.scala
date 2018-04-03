package stackOverFlow

import org.apache.spark._
import org.apache.spark.SparkContext._

object sortBy {
	def main(args:Array[String]) {
		    val sc = new SparkContext("local[*]", "sortBy")

				val input :Array[(Char, Int)] = Array(('a',1), ('b',2), ('c',1), ('d',3))
				val rdd = sc.parallelize(input)
				val result = rdd.sortBy(_._2,false)
				println(result.collect().mkString(","))
	}
}