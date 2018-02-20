package examples.Transformations

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FilterExample {
  def main(args:Array[String]) {
    val sc = new SparkContext("local[*]", "FilterExample")
    val x = sc.parallelize(Array(1,2,3))
    val y= x.filter(n => n%2 == 1)
    println(x.collect().mkString(","))
    println(y.collect().mkString(","))
  }
}