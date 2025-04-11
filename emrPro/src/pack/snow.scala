package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object snow {

  def main(args: Array[String]): Unit = {

    println("===Hello====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host", "localhost")
      .set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()

    import spark.implicits._

    val snowdf = spark.read.format("snowflake").option("sfURL", "https://vcnucps-qnb20226.snowflakecomputing.com").option("sfAccount", "vcnucps-qnb20226").option("sfUser", "Srini").option("sfPassword", "Srini@1a2b3#4!").option("sfDatabase", "SparkFlow").option("sfSchema", "SparkFlowSchema").option("sfRole", "ACCOUNTADMIN").option("sfWarehouse", "COMPUTE_WH").option("dbtable", "web_details").load()

    val aggdf = snowdf.groupBy("username").agg(count("site").as("cnt"))
    
    aggdf.show()
   
    aggdf.write.format("parquet").mode("overwrite").save("s3://zeyosrinis3/dest/sitecount")

  }
}