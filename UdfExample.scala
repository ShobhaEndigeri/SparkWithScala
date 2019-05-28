import spark.implicits._
case class Stuff(a:String,b:Int)
val d= sc.parallelize(Seq( ("a",1),("b",2),("",3) ,("d",4)).map { x => Stuff(x._1,x._2)  }).toDF

import org.apache.spark.sql.functions.udf

val func = udf( (s:String) => if(s.isEmpty) 0 else 1 )
val r = d.select( $"a", $"b", func($"a").as("notempty") )
