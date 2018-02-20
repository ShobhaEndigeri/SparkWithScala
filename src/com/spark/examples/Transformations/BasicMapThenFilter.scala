package examples.Transformations

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object BasicMapThenFilter {
  def main(args:Array[String]) {
    
    val sc = new SparkContext("local[*]", "BasicMapThenFilter")
    val input = sc.parallelize(List(1,2,3,4));
    val sqaured = input.map(x => (x*x))
    val result = sqaured.filter(x => x != 1)
    println(result.collect().mkString(","))
    
  }
}