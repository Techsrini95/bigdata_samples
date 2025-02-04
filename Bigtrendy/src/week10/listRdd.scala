package week10

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types._
import scala.io.Source

object listRdd {

  def main(args: Array[String]): Unit = {

    println("===Hello====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").
      set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().appName("RDD to DataFrame")
      .master("local").getOrCreate()

    val mylist = List(
      "WARN: Tuesday 4 September 0405",
      "ERROR: Tuesday 4 September 0408",
      "ERROR: Tuesday 4 September 0408",
      "ERROR: Tuesday 4 September 0408",
      "ERROR: Tuesday 4 September 0408",
      "ERROR: Tuesday 4 September 0408")

    val rdd = sc.parallelize(mylist)

    rdd.foreach(println)

    val rdd2 = rdd.map(x => x.split(":")(0))

    rdd2.foreach(println)

    val rdd3 = rdd2.map(x => (x, 1))

    rdd3.foreach(println)

    val redukey = rdd3.reduceByKey(_ + _)
    println
    println

    redukey.foreach(println)

  }
}