import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object AverageRatingsForMovie {
       /**
       * Given movie id and rating find out the average ratings for each movie
       * input sample
       * 101 abc 5
       * 102 xyz 4
       *  99 hij 3
       * 
       */
  
  def parseLine(line: String) = {
      val fields = line.split(",")
      val movieId = fields(1)
      val ratings = fields(3).toInt
      (movieId, ratings)
  }
  
  def main(args: Array[String]) {
        
    val sc = new SparkContext("local[*]", "AverageRatingsForMovie")
  
    val lines = sc.textFile("../input.csv")
    
    val rdd = lines.map(parseLine)
    
    
    val total = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))
    
    val averagesRatings = total.mapValues(x => x._1 / x._2)
    
    val results = averagesRatings.collect()
    
    results.sorted.foreach(println)
  }
    
}