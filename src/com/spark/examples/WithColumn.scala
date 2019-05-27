import org.apache.spark._
import org.apache.spark.SparkContext._

val sqlContext = new SQLContext(sc)
import sqlContext.implicits._ // for `toDF` and $""

val df = List(
  ("joao"),
  ("gabriel")
).toDF("first_name")

val df2 = df.withColumn(
  "greeting",
  lit("HEY!")
)

val df3 = df2.withColumn(
  "fav_activity",
  lit("surfing")
)

df3.show()
