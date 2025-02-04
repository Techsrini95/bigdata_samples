package datasample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types._

object sampledata {

  def main(args: Array[String]): Unit = {

    println("===Hello====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").
      set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().appName("RDD to DataFrame")
      .master("local").getOrCreate()

    val rdd = sc.textFile("C://Users/laksh/OneDrive/Desktop/spark/TrendySourcedata/week9/search_data.txt")

    val flatmaprdd = rdd.flatMap(x => x.split(" "))
    val maprdd = flatmaprdd.map(x => (x, 1))

    val reducuerdd = maprdd.reduceByKey(_ + _)

    val sortrdd = reducuerdd.sortBy(x => x._2, ascending = false)

    sortrdd.take(10).foreach(println)
//
//    val maprdd1 = maprdd.map(x => x._2)
//
//    val reducuerdd1 = maprdd1.reduce((x, y) => (x + y).toInt)
//
//    println
//    println
//    println("total sum   " + reducuerdd1)

  }

}