package datasample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types._

object moviedata {

  def main(args: Array[String]): Unit = {
    println("===Hello====")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]").
      set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().appName("RDD to DataFrame")
      .master("local").getOrCreate()

  }
}