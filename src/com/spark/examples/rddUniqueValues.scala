package examples

import org.apache.spark._
import org.apache.spark.SparkContext._

object rddUniqueValues {
	def main(args:Array[String]) {
		    val sc = new SparkContext("local[*]", "rddUniqueValues")


  /**
   * Input
   * 100, Array(1,2,3,4,5)

200,Array(1,2,50,20)

300, Array(30,2,400,1)

Output
(1,2,3,4,5,20,30,50,400)
   */
		    val input :Array[(Int, Array[Int])] = Array((100,Array(1,2,3,4,5)), (200,Array(1,7,8,4,5)), (300,Array(1,9,3,11,5)), (400,Array(10,12,13,4,5)))
		    val rdd = sc.parallelize(input)
		    val result = rdd.flatMap(_._2).distinct()
		    println(result.sortBy(x=>x).collect().mkString(","))
	}
}