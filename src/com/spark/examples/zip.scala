import org.apache.spark._
import org.apache.spark.SparkContext._

val x= sc.parallelize(Array(1,2,3))
val y= x.map(n=>n*n)
val z= x.zip(y)
println(z.collect().mkString(", "))
