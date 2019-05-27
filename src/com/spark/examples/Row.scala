import org.apache.spark.sql.Row
val row = Row(1, "hello")
println(row(1))
println(row.get(1))

val row1 = Row(1, "hello")
println(row.getAs[Int](0))
println(row.getAs[String](1))

println(Row.fromSeq(Seq(1, "hello")))
println(Row.fromTuple((0, "hello")))
println(Row.merge(Row(1), Row("hello")))
