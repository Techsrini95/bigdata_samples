package sample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Ascending

object Cust_order {
  def main(args: Array[String]): Unit = {

    // Set up SparkConf and SparkContext to initialize Spark application
    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host", "localhost")
      .set("spark.driver.allowMultipleContexts", "true")

    // Create a SparkContext instance using the configuration
    val sc = new SparkContext(conf)

    // Set the logging level to ERROR to minimize the log output
    sc.setLogLevel("ERROR")

    // Create a Spark session, needed for Spark SQL operations
    val spark = SparkSession.builder()
      .appName("WordCount")
      .master("local[*]") // Set the master to "local" to run Spark on your local machine
      .getOrCreate()

    // Set log level for the Spark session as well
    sc.setLogLevel("ERROR")

    // Load the CSV file into an RDD
    val rdd = sc.textFile("C://Users/laksh/OneDrive/Desktop/spark/TrendySourcedata/week9/customer_orders.csv")

    // Optionally print the first few lines of the dataset (currently commented out)
    // rdd.take(5).foreach(println)

    // Split each line by a comma to separate values (e.g., ID, order, price)
    val rdd1 = rdd.map(x => x.split(","))

    // Map the split data to a tuple with the customer ID (arr(0)) and order price (arr(2))
    val rdd2 = rdd1.map(arr => (arr(0), arr(2).toFloat))

    // Reduce the dataset by customer ID, summing up the order prices for each customer
    val results = rdd2.reduceByKey(_ + _)

    // Sort the results by order price in descending order
    val sortbyvalue = results.sortBy(x => math.ceil(x._2).toLong, ascending = false)

    // Take the top 5 customers and print them
    sortbyvalue.take(5).foreach(println)

  }
}