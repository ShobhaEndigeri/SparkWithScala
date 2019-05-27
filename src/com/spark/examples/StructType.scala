import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.types.{StructField, StructType}

val data = Seq(
  Row(1, "a"),
  Row(5, "z")
)

val schema = StructType(
  List(
    StructField("num", IntegerType, true),
    StructField("letter", StringType, true)
  )
)

val df = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  schema
)

df.show()
