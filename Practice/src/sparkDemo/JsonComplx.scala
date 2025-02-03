package sparkDemo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object JsonComplx {

  def main(args: Array[String]): Unit = {

    println("===Hello====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    val inputFile = "C:/Users/laksh/OneDrive/Desktop/spark/sample.json"
    val jsondf = spark.read.option("multiline", true).format("json").load(inputFile)

    jsondf.show()
    jsondf.printSchema()

    val strdf = jsondf.withColumn("page", expr("info.page"))
      .withColumn("resultinfo", expr("info.results"))
      .withColumn("country", expr("info.seed"))
      .withColumn("version", expr("info.version")).drop("info")

    strdf.show()
    //    strdf.printSchema()

    val resudf = strdf.withColumn("result1", explode_outer(col("results"))).drop("results")
    resudf.show()
    resudf.printSchema()

    val flattenedResultDF = resudf
      .withColumn("cell", col("result1.cell"))
      .withColumn("dob_age", col("result1.dob.age"))
      .withColumn("dob_date", col("result1.dob.date"))
      .withColumn("email", col("result1.email"))
      .withColumn("gender", col("result1.gender"))
      .withColumn("id_name", col("result1.id.name"))
      .withColumn("id_value", col("result1.id.value"))
      .withColumn("location_city", col("result1.location.city"))
      .withColumn("coordinates_lat", col("result1.location.coordinates.latitude"))
      .withColumn("coordinates_long", col("result1.location.coordinates.longitude"))
      .withColumn("location_country", col("result1.location.country"))
      .withColumn("location_state", col("result1.location.state"))
      .withColumn("location_postcode", col("result1.location.postcode"))
      .withColumn("street_name", col("result1.location.street.name"))
      .withColumn("street_number", col("result1.location.street.number"))
      .withColumn("timezone_description", col("result1.location.timezone.description"))
      .withColumn("timezone_offset", col("result1.location.timezone.offset"))
      .withColumn("username", col("result1.login.username"))
      .withColumn("password", col("result1.login.password"))
      .withColumn("md5", col("result1.login.md5"))
      .withColumn("salt", col("result1.login.salt"))
      .withColumn("sha1", col("result1.login.sha1"))
      .withColumn("sha256", col("result1.login.sha256"))
      .withColumn("uuid", col("result1.login.uuid"))
      .withColumn("name_first", col("result1.name.first"))
      .withColumn("name_last", col("result1.name.last"))
      .withColumn("name_title", col("result1.name.title"))
      .withColumn("nat", col("result1.nat"))
      .withColumn("phone", col("result1.phone"))
      .withColumn("plarge", col("result1.picture.large"))
      .withColumn("pmedium", col("result1.picture.medium"))
      .withColumn("pthumbnail", col("result1.picture.thumbnail"))
      .withColumn("registered_age", col("result1.registered.age"))
      .withColumn("registered_date", col("result1.registered.date"))
      .drop("result1")

    flattenedResultDF.show()
    flattenedResultDF.printSchema()
  }

}
