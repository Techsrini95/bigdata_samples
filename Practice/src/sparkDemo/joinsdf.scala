package sparkDemo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object joinsdf {

  def main(args: Array[String]): Unit = {

    println("===Hello====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._


    val jsondata = """{
                        "bid": 1,
                        "centre": "zeyobron",
                        "trainer" : "Sai",
                        "zeyoaddress": {
                        "permanent" : "hyderabad",
                        "temporary" : "chennai"
                        }
                        }"""

    val jsondata1 = """{
                        "firstname": "Rajeev",
                        "lastname": "Sharma",
                        "emailaddress": "rajeev@ezeelive.com",
                        "isalive": true,
                        "age": 30,
                        "heightcm": 185.2,
                        "billingaddress": {
                        "address": "502, Main Market, Evershine City, Evershine, Vasai East",
                        "city": "Vasai Raod, Palghar",
                        "state": "Maharashtra",
                        "postalcode": "401208"},
                        "dateofbirth": null}"""

    val jsondata2 = """{
                        "firstName": "Rajeev",
                        "lastName": "Sharma",
                        "emailAddress": "rajeev@ezeelive.com",
                        "isAlive": true,
                        "age": 30,
                        "heightCm": 185.2,
                        "billingAddress": {
                        "address": "502, Main Market, Evershine City, Evershine, Vasai East",
                        "city": "Vasai Raod, Palghar",
                        "state": "Maharashtra",
                        "postalCode": "401208"
                        },
                        "shippingAddress": {
                        "address": "Ezeelive Technologies, A-4, Stattion Road, Oripada, Dahisar East",
                        "city": "Mumbai",
                        "state": "Maharashtra",
                        "postalCode": "400058"
                        },
                        "dateOfBirth": null
                        }"""

    val jsonrdd = sc.parallelize(List(jsondata))
    val data = spark.read.option("multiline", "true").json(jsonrdd)
    val jsonrdd1 = sc.parallelize(List(jsondata1))
    val data1 = spark.read.option("multiline", "true").json(jsonrdd1)
    val jsonrdd2 = sc.parallelize(List(jsondata2))
    val data2 = spark.read.option("multiline", "true").json(jsonrdd2)

    data.show()
    data.printSchema()
    data1.show()
    data1.printSchema()
    data2.show()
    data2.printSchema()

    val flattendata2 = data2.select(
      "firstname",
      "lastname",
      "emailaddress", "isalive", "age", "heightcm", "dateofbirth",
      "billingaddress.address",
      "billingaddress.city",
      "billingaddress.state",
      "billingaddress.postalcode",
      "shippingAddress.address",
      "shippingAddress.city",
      "shippingAddress.state",
      "shippingAddress.postalcode")

    flattendata2.show
    flattendata2.printSchema()

    val flattendata = data.select(
      "bid",
      "centre",
      "trainer",
      "zeyoaddress.permanent",
      "zeyoaddress.temporary")

    flattendata.show
    flattendata.printSchema()

    val flattendata1 = data1.select(
      "firstname",
      "lastname",
      "emailaddress", "isalive", "age", "heightcm", "dateofbirth",
      "billingaddress.address",
      "billingaddress.city",
      "billingaddress.state",
      "billingaddress.postalcode")

    flattendata1.show
    flattendata1.printSchema()

  }

}
