package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import scala.io.Source
import io.delta.tables._

object deltadata {

  def main(args: Array[String]): Unit = {

    println("===Hello====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host", "localhost")
      .set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()

    import spark.implicits._

    val urldata = Source.fromURL("https://randomuser.me/api/0.8/?results=15").mkString

    println(urldata)

    val rdd = sc.parallelize(List(urldata))

    val df = spark.read.option("multiline", "true").json(rdd)

    df.show()

    df.printSchema()

    val explodedf = df.withColumn("results", expr("explode(results)"))

    explodedf.show()

    explodedf.printSchema()

    val flattendf = explodedf.select(
      "nationality",
      "results.user.cell",
      "results.user.dob",
      "results.user.email",
      "results.user.gender",
      "results.user.location.city",
      "results.user.location.state",
      "results.user.location.street",
      "results.user.location.zip",
      "results.user.md5",
      "results.user.name.first",
      "results.user.name.last",
      "results.user.name.title",
      "results.user.password",
      "results.user.phone",
      "results.user.picture.large",
      "results.user.picture.medium",
      "results.user.picture.thumbnail",
      "results.user.registered",
      "results.user.salt",
      "results.user.sha1",
      "results.user.sha256",
      "results.user.username",
      "seed",
      "version")

    flattendf.show()
    flattendf.printSchema()

    // flattendf.write.format("parquet").mode("overwrite").save("s3://zeyosrinis3/dest/customer_api")

  }

}