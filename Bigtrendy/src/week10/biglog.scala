package week10

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types._
import scala.io.Source

object biglog {

  def main(args: Array[String]): Unit = {

    println("===Hello====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").
      set("spark.driver.host", "localhost").set("spark.ui.port", "4050")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().appName("RDD to DataFrame")
      .master("local").getOrCreate()

    val rdd = sc.textFile("C://Users/laksh/OneDrive/Desktop/spark/TrendySourcedata/week10/bigLog.txt")

    //    rdd.take(10).foreach(println)

    val splitdata = rdd.map(x => {

      val fields = x.split(":")
      val key = fields(0)
      val value = fields(1)
      (key, 1)
    })

    val reducedata = splitdata.reduceByKey(_ + _)

    reducedata.take(10).foreach(println)

    println("Number of partitions: " + reducedata.getNumPartitions)

    //    splitdata.groupByKey().collect().foreach(x => println(x._1, x._2.size))

    //    scala.io.StdIn.readLine()

 
  }
}