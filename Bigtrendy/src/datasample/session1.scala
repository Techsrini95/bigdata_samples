package datasample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object session1 {

  case class Person(ID: Int, Name: String, Age: Int, Score: Int)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host", "localhost")
      .set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val rdd = sc.textFile("C://Users/laksh/OneDrive/Desktop/spark/TrendySourcedata")
    rdd.take(5).foreach(println)

    val fmaprdd = rdd.flatMap(x => x.split("::"))

    fmaprdd.take(5).foreach(println)

    val maprd = fmaprdd.map(x => (x.toLowerCase(), 1))

    maprd.take(5).foreach(println)

    println
    println

    val reducerdd = maprd.reduceByKey((x, y) => x + y)
    reducerdd.take(5).foreach(println)

    println
    println

    val sortrdd = reducerdd.sortBy(_._2, ascending = false)
    sortrdd.take(5).foreach(println)

  }

}