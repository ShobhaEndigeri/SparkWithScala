package examples.Transformations

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object BasicMap {
  def main(args:Array[String]) {
    
    val sc = new SparkContext("local[*]", "BasicMap")
    var input = sc.parallelize(List(1,2,3,4));
    var result = input.map(x => (x*x))
    println(result.collect().mkString(","))
    
  }
}