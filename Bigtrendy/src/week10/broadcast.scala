package week10

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types._
import scala.io.Source

object broadcast {

  //  loading boaring words in local becuse not want be distributed across machines
  def loadingBoaringwords(): Set[String] = {
    var boardingWords: Set[String] = Set()
    val lines = Source.fromFile("C://Users/laksh/OneDrive/Desktop/spark/TrendySourcedata/week10/boringwords.txt").getLines()
    for (line <- lines) {
      boardingWords += line
    }
    boardingWords
  }

  def main(args: Array[String]): Unit = {

    println("===Hello====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").
      set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().appName("RDD to DataFrame")
      .master("local").getOrCreate()

    var nameset = sc.broadcast(loadingBoaringwords)

    val rdd = sc.textFile("C://Users/laksh/OneDrive/Desktop/spark/TrendySourcedata/week10/bigdata_campaign_data.csv")

    val mapinput = rdd.map(x => (x.split(",")(10).toDouble, x.split(",")(0)))
    val words = mapinput.flatMapValues(x => x.split(" "))

    val finalmap = words.map(x => (x._2.toLowerCase(), x._1))

    val filetrmap = finalmap.filter(x => !nameset.value(x._1))

    val total = filetrmap.reduceByKey(_ + _)

    val sortedrdd = total.sortBy(x => x._2,false)
     
    sortedrdd.take(20).foreach(println)

  }
}