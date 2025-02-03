package sparkDemo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Upper

object RddtoDF {

  def main(args: Array[String]): Unit = {

    println("===Hello====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    //    val inputcsv = "C:/Users/laksh/OneDrive/Desktop/spark/file3.txt"
    //    val inputparque = "C:/Users/laksh/OneDrive/Desktop/spark/file5.parquet"
    //    val inputorc = "C:/Users/laksh/OneDrive/Desktop/spark/demo.orc"
    //    val inputjson = "C:/Users/laksh/OneDrive/Desktop/spark/file4.json"

    val df = Seq(
      (1, "raj"),
      (2, "ravi"),
      (3, "sai"),
      (5, "rani")).toDF("id", "name")

    df.show()

    val df1 = Seq(
      (1, "mouse"),
      (3, "mobile"),
      (7, "laptop")).toDF("id", "product")

    df1.show()

    val joindf1 = df.join(df1, Seq("id"), "left")
    joindf1.show()

    val joindf2 = df.join(df1, Seq("id"), "right")
    joindf2.show()

    val joindf3 = df.join(df1, Seq("id"), "full")
    joindf3.show()

    val joindf4 = df.join(df1, Seq("id"), "inner")
    joindf4.show()

    val joindf5 = df.join(df1, Seq("id"), "left_anti")
    joindf5.show()

    val joindf6 = df.join(df1, Seq("id"), "left_semi")
    joindf6.show()

    println("==================joiiiin=============================")

    val df2 = spark.createDataFrame(Seq(
      (1, "raj", 28, "India", "Engineer", 50000, "M", "A+", "Active", "Yes"),
      (2, "ravi", 34, "India", "Doctor", 70000, "M", "B+", "Active", "No"),
      (3, "sai", 29, "USA", "Lawyer", 80000, "M", "O+", "Inactive", "Yes"),
      (4, "rani", 25, "UK", "Teacher", 60000, "F", "A-", "Active", "No"),
      (5, "john", 40, "USA", "Scientist", 90000, "M", "B+", "Active", "Yes"),
      (6, "lucy", 32, "Canada", "Artist", 45000, "F", "A-", "Inactive", "No"),
      (7, "mark", 38, "UK", "Teacher", 55000, "M", "O-", "Active", "Yes"),
      (8, "lisa", 29, "USA", "Engineer", 72000, "F", "AB+", "Active", "No"),
      (9, "mike", 26, "India", "Doctor", 65000, "M", "B-", "Inactive", "Yes"),
      (10, "susan", 35, "Germany", "Designer", 85000, "F", "A+", "Active", "Yes"),
      (11, "jake", 27, "Australia", "Architect", 78000, "M", "O+", "Active", "No"),
      (12, "emily", 33, "Canada", "Nurse", 56000, "F", "B-", "Inactive", "Yes"),
      (13, "alex", 41, "India", "Lawyer", 92000, "M", "AB-", "Active", "No"),
      (14, "mia", 24, "USA", "Photographer", 48000, "F", "A-", "Inactive", "Yes")))
      .toDF("id", "name", "age", "country", "profession", "salary", "gender", "bgrp", "status", "married")

    val df3 = spark.createDataFrame(Seq(
      (1, "mouse", "electronics", "A+", 5),
      (3, "mobile", "electronics", "O+", 10),
      (5, "laptop", "electronics", "B-", 7),
      (6, "tablet", "electronics", "A-", 12),
      (7, "keyboard", "electronics", "AB+", 8),
      (8, "headphones", "electronics", "O-", 15),
      (9, "monitor", "electronics", "B-", 3),
      (10, "printer", "electronics", "A+", 9),
      (11, "mousepad", "electronics", "O+", 6),
      (12, "webcam", "electronics", "AB-", 4)))
      .toDF("pid", "product", "category", "bgrp", "stock")

    //val reDf2 = df2.selectExpr("bgrp as egrp") // Rename df2's bgrp to egrp
    //val reDf3 = df3.selectExpr("bgrp as pgrp") // Rename df3's bgrp to pgrp

    val reDf2 = df2.withColumnRenamed("bgrp", "egrp") // Rename df2's bgrp to egrp
    val reDf3 = df3.withColumnRenamed("bgrp", "pgrp") // Rename df3's bgrp to pgrp

    reDf2.show()
    reDf3.show()

    val joindff = reDf2.join(reDf3, reDf2("id") === reDf3("pid"), "left")
    joindff.show()
    joindff.printSchema()

    println("==================withColumn=============================")

    val transdf = joindff.withColumn("id", lpad(col("id"), 5, "0").cast("int"))
      .withColumn("name", rpad(upper(col("name")), 5, "x"))
      .withColumn("country", when(col("country") === "India", "IND")
        .when(col("country") === "USA", "US")
        .when(col("country") === "UK", "UN")
        .when(col("country") === "Canada", "CA")
        .when(col("country") === "Germany", "GE")
        .when(col("country") === "Australia", "AU"))
      .withColumn("profession", upper(col("profession")))
      .withColumn("gender", when(col("gender") === "M", "Male").otherwise("Female"))
      .withColumn("status", when(col("status") === "Active", "Yes").otherwise("No"))
      .withColumn("pid", when(col("id").isNull, "NA").otherwise(lpad(col("id"), 5, "0")).cast("int"))
      .na.fill(Map("product" -> "no-prod"))
      .withColumn("category", when(col("category").isNull, "no-electr").otherwise(col("category")))
      .withColumn("pgrp", coalesce(col("pgrp"), lit("NO")))
      .withColumn("stock", lpad(coalesce(col("stock"), lit("0")), 2, "0").cast("int"))

    transdf.show()
    transdf.printSchema()

    println("==================Finish=============================")

    //              val transdf = df.selectExpr(
    //                "id",
    //                "age",
    //                """
    //                  CASE
    //                    WHEN age < 18 THEN 'Minor'
    //                    WHEN age >= 18 AND age <= 40 THEN 'Adult'
    //                    ELSE 'Senior'
    //                  END AS age_group
    //                """)

    //        val transdf2 = df.withColumn("age_group",
    //          when(col("age") < 18, "Minor")
    //            .when(col("age").between(18, 40), "Adult")
    //            .otherwise("Senior"))

    //    val seldf = transdf.select("id" as "eid", "name", "age", "country", "pid", "product", "category", "ebgrp").show()

  }

}
