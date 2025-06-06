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

    val rdd = sc.textFile("C://Users/laksh/OneDrive/Desktop/spark/TrendySourcedata/week9/movie_data.data")

    val rdd1 = rdd.map(x => x.split("\t")(2))
    val finalre = rdd1.countByValue()

    //    val rdd2 = rdd1.map(x => (x, 1))
    //
    //    val finalre = rdd2.reduceByKey(_+_)

    finalre.take(5).foreach(println)

  }
}