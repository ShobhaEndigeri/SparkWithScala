package com.sundogsoftware.spark

import org.apache.spark.SparkContext

object MovieWithHighestRatings {
  
	def main(args:Array[String]){
	  
		val sc = new SparkContext("local[*]","MovieWithHighestRatings");
		
		/****
		 * 
		 * 	movieId userId ratings
		 * 	313	    673	    4	
				58	    109	    4	
				270			781			5	
				13			476			2	
				189			1				5	
		 */
		val lines = sc.textFile("../input.data");
		//map (movieId , ratings)
		val input = lines.map(x => (x.split("\t")(1).toInt,x.split("\t")(2).toInt));
		
		val output = input.reduceByKey((x,y) => (x+y))
		
		val flippedOut = output.map(x=> (x._2,x._1)).sortByKey(false)
		
		val famousMovie = flippedOut.first()
				
		println("Movie id "+famousMovie._2+" has got maximum ratings ="+famousMovie._1)
	}

}