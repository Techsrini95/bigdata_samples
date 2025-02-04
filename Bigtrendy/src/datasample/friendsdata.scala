package datasample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types._

object friendsdata {

  // row_id,name,age,no_connections

  //  def parLine(Line:String) {
  //
  //    val fields =Line.split("::")
  //    val age = fields(2).toInt
  //    val num_conn = fields(3).toInt
  //    (age,num_conn)
  //  }

  def main(args: Array[String]): Unit = {

    println("===Hello====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").
      set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().appName("RDD to DataFrame")
      .master("local").getOrCreate()

    val rdd = sc.textFile("C://Users/laksh/OneDrive/Desktop/spark/TrendySourcedata/week9/friends.csv")

    rdd.take(10).foreach(println)

    //    val rdd1 = rdd.map(parLine)

    val rdd1 = rdd.map(x => x.split("::"))
    val rdd3 = rdd1.map(x => (x(2).toInt, x(3).toInt))

    rdd3.take(10).foreach(println)

    val rdd4 = rdd3.map(x => (x._1, (x._2, 1)))
    rdd4.take(10).foreach(println)

    //    when you use reduce by key the index will only considered for values not for key so
    //    (33,(385,1)) 385 - x._1 , 1 = x._2

    val reducerdd = rdd4.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    println
    println
    reducerdd.take(10).foreach(println)

    println
    println
    val finalrdd = reducerdd.map(x => (x._1, x._2._1 / x._2._2))
    finalrdd.take(10).foreach(println)

    println
    println

    val sortbyvalue = finalrdd.sortBy(x => x._2, ascending = false)
    sortbyvalue.take(10).foreach(println)

  }
}