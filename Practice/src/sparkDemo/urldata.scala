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

object urldata {

  def main(args: Array[String]): Unit = {
    println("===Hello====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    val urldata = Source.fromURL("https://randomuser.me/api/0.8/?results=1").mkString
    println(urldata)

    implicit val formats: DefaultFormats.type = DefaultFormats
    val json = parse(urldata)

    // Extract and count the users
    val users = (json \ "results").extract[List[JValue]]
    val userCount = users.size

    println(s"Number of users: $userCount")
  }
}