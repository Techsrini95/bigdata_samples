package sparkDemo

import org.apache.spark.sql.SparkSession

object sampleConf {

  def main(args: Array[String]): Unit = {

    println("hello world")

    // Initialize SparkSession
    val spark = SparkSession.builder
      .appName("SimpleSparkApp")
      .master("local[*]") // Run locally with all available cores
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Load a text file
    val inputFile = "C:/Users/laksh/OneDrive/Desktop/spark/input.txt" // Replace with your file path
    val textFile = spark.sparkContext.textFile(inputFile)

    // Perform a word count
    val wordCounts = textFile
      .flatMap(line => line.split("\\s+")) // Split lines into words
      .map(word => (word, 1)) // Map each word to a pair (word, 1)
      .reduceByKey(_ + _) // Reduce pairs by key to count words

    // Collect and print results
    wordCounts.collect().foreach { println }
    //      case (word, count) =>
    //        println(s"$word: $count")
    //    }

    // Stop SparkSession
    spark.stop()

  }
}
