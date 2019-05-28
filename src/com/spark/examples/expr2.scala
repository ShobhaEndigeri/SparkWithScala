import org.apache.spark._
import org.apache.spark.SparkContext._

val sqlContext = new SQLContext(sc)
import sqlContext.implicits._ // for `toDF` and $""

val df = Seq((1,2)).toDF("x","y")

val myExpression = "x+y"

import org.apache.spark.sql.functions.expr

df.withColumn("z",expr(myExpression)).show()
