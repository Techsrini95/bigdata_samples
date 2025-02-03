package sparkDemo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import scala.io.Source
import org.json4s._
import org.json4s.native._
import org.json4s.native.JsonMethods._

object urldataread {

  def main(args: Array[String]): Unit = {
    println("===Hello====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    val urldata = Source.fromURL("https://randomuser.me/api/0.8/?results=100").mkString
    println(urldata)

    //val rdd = spark.sparkContext.parallelize(urldata)

    val rdd = sc.parallelize(List(urldata))

    val df = spark.read.option("multiline", "true").json(rdd)

    df.show()
    df.printSchema()

    //    val explodedf = df.withColumn("results", expr("explode(results)"))
    //    explodedf.show()
    //    explodedf.printSchema()

    val explodedDf = df.select(col("nationality"), explode(col("results")).alias("result"))

    explodedDf.show()
    explodedDf.printSchema()

    val strdf = explodedDf.select(
      col("nationality"),
      col("result.user.cell"),
      col("result.user.dob"),
      col("result.user.email"),
      col("result.user.gender"),
      col("result.user.location.city"),
      col("result.user.location.state"),
      col("result.user.location.street"),
      col("result.user.location.zip"),
      col("result.user.md5"),
      col("result.user.name.first"),
      col("result.user.name.last"),
      col("result.user.name.title"),
      col("result.user.password"),
      col("result.user.phone"),
      col("result.user.picture.large"),
      col("result.user.picture.medium"),
      col("result.user.picture.thumbnail"),
      col("result.user.registered"),
      col("result.user.sha1").alias("audit1"),
      col("result.user.sha256").alias("audit2"),
      col("result.user.username"))

    strdf.show()

    // Rename columns to uppercase
    val upperCaseDf = strdf.columns.foldLeft(strdf) { (df, columnName) =>
      df.withColumnRenamed(columnName, columnName.toUpperCase)
    }

    upperCaseDf.show()

    upperCaseDf.write
      .option("header", "true")
      .mode("overwrite")
      .csv("C:/Users/laksh/OneDrive/Desktop/spark/dataexport/csvdatasmaple")

  }
}