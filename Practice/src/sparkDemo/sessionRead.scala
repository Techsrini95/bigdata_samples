package sparkDemo

import org.apache.spark.sql.SparkSession


object sessionRead {

 

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Example").master("local[*]").getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val list = List("Alice", "Bob", "Charlie")

    val listRDD = sc.parallelize(list)

    listRDD.collect().foreach(println) // Output: Alice, Bob, Charlie

    import spark.implicits._

    val listDF = listRDD.toDF("name")
    listDF.show()

    // Example with multiple columns using case class

  }
}



