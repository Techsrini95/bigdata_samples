package sparkDemo
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark._

object sparsession {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val filepath = "C:/Users/laksh/OneDrive/Desktop/spark/usdata.csv"
    val data = sc.textFile(filepath)

    data.take(5).foreach(println)

    println

    val flatmap = data.flatMap(x => x.split(",")).map(x => x.replace("-", ""))

    flatmap.take(10).foreach(println)
    println
    println("phone numbers ===============================================================================")
    println

    val phone = flatmap.filter(word => word.forall(_.isDigit) && word.length ==10)
      .map(x => (x, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false)

    phone.take(10).foreach(println)

    println
    println("emaildata")
    println
    val edata = flatmap.filter(x => x.exists(_.isLetter) && x.contains("@"))
      .map(x => (x, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false)

    edata.take(10).foreach(println)

    println
    println("webdata")
    println
    val webdata = flatmap.filter(x => x.exists(_.isLetter) && x.contains("http://www"))
      .map(x => x.replace("http://", "")).map(x => (x, 1))
      .reduceByKey(_ + _).sortBy(_._2, ascending = false)

    webdata.take(10).foreach(println)

    println
    println("zeyodata")
    println

    val lendata = data.filter(x => x.length > 200)

    println

    println("====length data=====")

    println

    lendata.foreach(println)

    val flatten = lendata.flatMap(x => x.split(","))

    println

    println("====flatten data=====")

    println

    flatten.foreach(println)

    val repdata = flatten.map(x => x.replaceAll("[\"-]", "")).map(_.trim())

    println
    println("====repdata data=====")
    println

    repdata.foreach(println)

    val condata = repdata.map(x => x + ",zeyo")

    println

    println("========condata=========")

    println

    condata.foreach(println)
  }
}

