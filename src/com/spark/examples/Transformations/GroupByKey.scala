package examples.Transformations

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object GroupByKey {
  def main(args:Array[String]) {
	 	val sc = new SparkContext("local[*]", "GroupByKey")
		val x= sc.parallelize(Array(('B',5),('B',4),('A',3),('A',2),('A',1)))
		val y= x.groupByKey()
		println(x.collect().mkString(", "))
		println(y.collect().mkString(", "))
  }
}
