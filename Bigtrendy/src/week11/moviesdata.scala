package week11

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types._
import scala.io.Source

object moviesdata {

  def main(args: Array[String]): Unit = {

    println("===Hello====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").
      set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().appName("RDD to DataFrame")
      .master("local").getOrCreate()

    val rdd1 = sc.textFile("C://Users/laksh/OneDrive/Desktop/spark/TrendySourcedata/week11/ratings.dat")
    //    rdd2.take(10).foreach(println)

    val splitrdd = rdd1.map(lines => {

      val split = lines.split("::")
      val movieid = split(1)
      val ratings = split(2)

      (movieid, ratings)
    })

    splitrdd.take(5).foreach(println)

    val maprdd = splitrdd.map(x => (x._1, (x._2.toFloat, 1.0)))
    maprdd.take(5).foreach(println)

    val reducerdd = maprdd.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    reducerdd.take(5).foreach(println)

    println
    println

    val filterrdd = reducerdd.filter(x => x._2._2 > 100)
    filterrdd.take(5).foreach(println)

    println
    println

    val avgratings = filterrdd.mapValues(x => (x._1 / x._2)).filter(x => x._2 > 4.5)
    avgratings.take(5).foreach(println)

    println
    println

    val rdd2 = sc.textFile("C://Users/laksh/OneDrive/Desktop/spark/TrendySourcedata/week11/movies.dat")
    //    rdd.take(10).foreach(println)

    val splitrdd2 = rdd2.map(lines => {

      val split = lines.split("::")
      val movieid = split(0)
      val ratings = split(1)

      (movieid, ratings)
    })
    println
    println
    splitrdd2.take(5).foreach(println)

    val joinrdd = splitrdd2.join(avgratings)

    println
    println
    joinrdd.take(5).foreach(println)

    val topmovies = joinrdd.map(x => x._2._1)
    println
    println
    topmovies.take(5).foreach(println)

  }

}