package examples

import org.apache.spark._
import org.apache.spark.SparkContext._

object GroupByKeyWordCount {
  def main(args:Array[String]) {
    
    val sc = new SparkContext("local[*]", "ReduceByKey")
		val words = Array("one", "two", "two", "three", "three", "three")
		val wordPairsRDD = sc.parallelize(words).map(word => (word,1))
		val wordCountsWithGroup = wordPairsRDD.groupByKey().map(t => (t._1 , t._2.sum))
		println(wordCountsWithGroup.collect().mkString(", "))

    }
}