package com.spark.examples

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object TotalAmountSpentByCustomer {
	/**
	 * input sample
	 * 	85	1733	28.53
			53		9900	83.55
			14		1505	4.32
			51		3378	19.8
			42		6926	57.77
	 * 
	 */
	def extractCustomeridAmountPair(line: String) = {
			val fields = line.split(",")
					(fields(0).toInt, fields(2).toFloat)
	}

	def main(args: Array[String]) {

		Logger.getLogger("org").setLevel(Level.ERROR)

		val sc = new SparkContext("local[*]", "TotalAmountSpentByCustomer")   

		val input = sc.textFile("../input.csv")

		val mappedInput = input.map(extractCustomeridAmountPair)

		val totalByCustomer = mappedInput.reduceByKey( (x,y) => x + y )

		val results = totalByCustomer.collect()

		results.foreach(println)
	}

}

