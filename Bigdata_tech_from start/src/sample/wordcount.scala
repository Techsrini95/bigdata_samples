package sample

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark._

object wordcount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host", "localhost")
      .set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("WordCount")
      .master("local[*]") // Set the master to "local" to run Spark on your local machine
      .getOrCreate()

    sc.setLogLevel("ERROR")
    // Read some input data as an RDD (Resilient Distributed Dataset)
    val inputData = spark.sparkContext.parallelize(Seq(
      "hello world",
      "hello from spark",
      "spark is amazing",
      "hello spark world"))

    // Perform a flatMap transformation to split each line into words
    val words = inputData.flatMap(line => line.split(" "))

    // Transform the words into lowercase and then count the occurrences
    val wordCounts = words.map(word => (word.toLowerCase, 1))
      .reduceByKey(_ + _) // Reduces by key (the word) and sums the occurrences

    // Collect and print the results
    val results = wordCounts.collect()
    results.foreach(println)

    // Stop the Spark session
    spark.stop()
  }
}


