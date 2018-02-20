package examples.Transformations

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object ReduceByKey {
  def main(args:Array[String]) {
    
    val sc = new SparkContext("local[*]", "ReduceByKey")
		val words = Array("one", "two", "two", "three", "three", "three")
		val wordPairsRDD = sc.parallelize(words).map(word => (word,1))
		val valwordCountsWithReduce = wordPairsRDD.reduceByKey(_+_)
		println(valwordCountsWithReduce.collect().mkString(", "))
		
  }
}