package com.spark.examples

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object GroupBy {
  def main(args:Array[String]) {
		Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "GroupBy")
		val x = sc.parallelize(Array("John","Fred","Anna","James"))
		val y = x.groupBy(ele => ele.charAt(0))
		println(y.collect().mkString(", "))
  }
  
}