
import org.apache.spark.SparkContext
import org.apache.spark.sql._

object SparkSQLFruits {

case class Fruits(id:Int,name:String,quantity:Int)

def main(args:Array[String]) {

	val spark = SparkSession
			.builder
			.appName("SparkSQL")
			.master("local[*]")
			.config("spark.sql.warehouse.dir", "file:///C:/temp")
			.getOrCreate()

			import spark.implicits._

			val fruits = spark.sparkContext.textFile("../fruits.txt").map(_.split(",")).map(x=>Fruits(x(0).toInt,x(1),x(2).toInt)).toDS

			fruits.registerTempTable("fruits")
			
			val records = spark.sqlContext.sql("SELECT * FROM fruits")
			
			records.foreach(x=>println(x))
  }
}