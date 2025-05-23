package datasample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types._

object session2 {

  def main(args: Array[String]): Unit = {
    println("===Hello====")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]").
      set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().appName("RDD to DataFrame")
      .master("local").getOrCreate()

    import spark.implicits._

    val data = Seq(
      (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
      (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
      (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
      (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
      (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
      (5, "02-14-2011", 200.0, "Gymnastics", null, "cash"),
      (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
      (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
      (8, "02-14-2011", 200.0, "Gymnastics", null, "cash"))

    val df = data.toDF("id", "tdate", "amount", "category", "product", "spendby")
    df.printSchema()
    df.show()

  }
}