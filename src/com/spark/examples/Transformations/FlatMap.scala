package examples.Transformations
  
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FlatMap1 {
  def main(args:Array[String]) {
    val sc = new SparkContext("local[*]", "FlatMap")
    val x = sc.parallelize(Array(1,2,3))
    val y = x.flatMap(n => Array(n, n*100, 42))
    println(x.collect().mkString(","))
    println(y.collect().mkString(","))
  }
  
}