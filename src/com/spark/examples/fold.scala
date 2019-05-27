import org.apache.spark._
import org.apache.spark.SparkContext._

val rdd1 = sc.parallelize(List(1,2,3,4,5), 3)
rdd1.fold(5)(_ + _)
