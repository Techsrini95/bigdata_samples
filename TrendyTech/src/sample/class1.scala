package sample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession._

object class1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println("welcome")
    
    val rdd = sc.textFile("File:///C:/Users/laksh/OneDrive/Desktop/spark/TrendySourcedata/friendsdata.csv")

     
     
     val splitrdd= rdd.flatMap(x => x.split("::"))
     
     val maprdd = splitrdd.map(x => (x ,1))
     
     val mapreduceBy = maprdd.reduceByKey(_+_)
     
     
     
     mapreduceBy.collect().take(100).foreach(println) 
    
  }
}