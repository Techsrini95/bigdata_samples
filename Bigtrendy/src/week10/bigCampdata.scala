package week10

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types._

object bigCampdata {

  def main(args: Array[String]): Unit = {

    println("===Hello====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").
      set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().appName("RDD to DataFrame")
      .master("local").getOrCreate()

    val rdd = sc.textFile("C://Users/laksh/OneDrive/Desktop/spark/TrendySourcedata/week10/bigdata_campaign_data.csv")
    //    rdd.foreach(println)

    //    val mappedInput = initial_rdd.map(x =>
    //(x.split(",")(10).toFloat,x.split(",")(0)))

    val rdd1 = rdd.map(line => {
      val splitrdd = line.split(",")
      val abc = splitrdd(10)
      val xyz = splitrdd(0)
      (abc, xyz)
    })
    //
    //    rdd1.take(10).foreach(println)

    val flatrdd = rdd1.flatMapValues(x => x.split(" "))

    //    val sortkey = flatrdd.sortByKey(ascending = false)

    val finalrdd = flatrdd.map(x => (x._2, x._1))

    //    val filterrdd= finalrdd.filter((abc, xyz) => xyz > 200)

    val filterrdd = finalrdd.filter { case (abc, xyz) => xyz.forall(_.isDigit) && xyz.toInt > 0 }

    val reducerdd = filterrdd.mapValues(_.toInt).reduceByKey(_ + _)

    val sorted = reducerdd.sortBy(x => x._2, false)

    sorted.take(10).foreach(println)

  }
}