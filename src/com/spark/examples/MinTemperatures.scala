package com.spark.examples

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min

object MinTemperatures {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat
    (stationID, entryType, temperature)
  }
  
  def main(args: Array[String]) {
       
    val sc = new SparkContext("local[*]", "MinTemperatures")
    
    val lines = sc.textFile("../input.csv")
    
    val parsedLines = lines.map(parseLine)
    
    val minTemps = parsedLines.filter(x => x._2 == "TMIN")
    
    val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))
    
    val minTempsByStation = stationTemps.reduceByKey( (x,y) => min(x,y))
    
    val results = minTempsByStation.collect()
    
    for (result <- results.sorted) {
       val station = result._1
       val temp = result._2
       println(s"$station minimum temperature: $temp")
    }
      
  }
}