package sparkDemo

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark._

object tuplesdf {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder()
      .appName("Tuple to DataFrame").master("local").getOrCreate()
    // Single tuple

    val tuple = (1, "Alice", 25)

    // Multiple tuples in a sequence

    val tuples = Seq((1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 28))

    val tupleRDD = sc.parallelize(tuples)

    tupleRDD.collect().foreach(println) // Output: (1, Alice, 25), (2, Bob, 30), (3, Charlie, 28)

    import spark.implicits._

    // Define a case class
    //case class Person(id: Int, name: String, age: Int)

    // Convert sequence of tuples to DataFrame using case class
    val tupleDF = tuples.toDF("id", "name", "age")

    tupleDF.show()
  }
}