package sparkDemo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object jsondf {
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

    val flattenedDf = jsondf
      .select(
        col("info.page"),
        col("info.results"),
        col("info.seed"),
        col("info.version"),
        // Flattening the array `results`
        explode(col("results")).alias("result"))

    flattenedDf.show(truncate = false)
    flattenedDf.printSchema()

    // Selecting fields after exploding the "results" array
    val seledf = flattenedDf.select(
//      col("page"),
//      col("results"),
//      col("seed"),
//      col("version"),
      col("result.cell"),
      col("result.dob.age").alias("dob_age"),
      col("result.dob.date").alias("dob_date"),
      col("result.email"),
      col("result.gender"),
      col("result.id.name").alias("id_name"),
      col("result.id.value").alias("id_value"),
      col("result.location.city"),
      col("result.location.coordinates.latitude").alias("latitude"),
      col("result.location.coordinates.longitude").alias("longitude"),
      col("result.location.country"),
      col("result.location.postcode"),
      col("result.location.state"),
      col("result.location.street.name").alias("street_name"),
      col("result.location.street.number").alias("street_number"),
      col("result.location.timezone.description").alias("timezone_description"),
      col("result.location.timezone.offset").alias("timezone_offset"),
      col("result.login.md5"),
      col("result.login.password"),
      col("result.login.salt"),
      col("result.login.sha1"),
      col("result.login.sha256"),
      col("result.login.username"),
      col("result.login.uuid"),
      col("result.name.first").alias("first_name"),
      col("result.name.last").alias("last_name"),
      col("result.name.title").alias("title"),
      col("result.nat"),
      col("result.phone"),
      col("result.picture.large").alias("picture_large"),
      col("result.picture.medium").alias("picture_medium"),
      col("result.picture.thumbnail").alias("picture_thumbnail"),
      col("result.registered.age").alias("registered_age"),
      col("result.registered.date").alias("registered_date"))

//    seledf.show(truncate = false)
//    seledf.printSchema()

  }
}