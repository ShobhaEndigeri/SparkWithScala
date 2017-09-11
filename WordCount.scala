object WordCount {
 
  def main(args: Array[String]) {
   
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "WordCount")   
    
    val input = sc.textFile("../input.txt")
    
    val words = input.flatMap(x => x.split("\\W+"))
    
	val lowercaseWords = words.map(x => x.toLowerCase()).map(x  => (x,1)).reduceByKey((x,y) => x+y)    
    
	lowercaseWords.foreach(println)
  }
  
}