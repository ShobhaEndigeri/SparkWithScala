    import spark.implicits._ //so that you could use .toDF
    val df = Seq(
      ("steak", 1, 1, 150),
      ("steak", 2, 2, 180),
      ("fish", 3, 3, 100)
    ).toDF("C1", "C2", "C3", "C4")

    import org.apache.spark.sql.functions._

    // 1st approach using expr
    df.withColumn("C5", expr("C2/(C3 + C4)")).show()

    // 2nd approach using selectExpr
    df.selectExpr("*", "(C2/(C3 + C4)) as C5").show()
