package examples.Transformations

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FoldExample {
  def main(args:Array[String]) {
    
    val sc = new SparkContext("local[*]", "FoldExample")
    val input = sc.parallelize(List(1,2,3,4));
    val result = input.fold(1)((x,y) => (x*y));
    println(result)
    
  }
}