package sample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object dataeng extends App {

  val conf = new SparkConf().setAppName("first").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  println("===Hello====")

  val input = sc.textFile("File:///C:/Users/laksh/OneDrive/Desktop/customer_order.csv")
  val inputsplit = input.map(x => (x.split(",")(0), x.split(",")(2).toFloat))
    .reduceByKey(_ + _).sortBy(x => x._2 ,false)

  inputsplit.collect().take(5).foreach(println)

}