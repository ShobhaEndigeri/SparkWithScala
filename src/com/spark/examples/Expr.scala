import org.apache.spark._
import org.apache.spark.SparkContext._

val sqlContext = new SQLContext(sc)
import sqlContext.implicits._ // for `toDF` and $""

val df = List(
    (1,1,1,1,3),
    (2,2,3,4,4)
  ).toDF("colA", "colB", "colC", "colD", "colE")

val myExpression = "colA<colC"

import org.apache.spark.sql.functions.expr

df.withColumn("colRESULT",expr(myExpression)).show()
