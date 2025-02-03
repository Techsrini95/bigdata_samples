package sparkDemo

import org.apache.spark._
import org.apache.spark.sql.catalyst.expressions.Ascending

object practice2 {

  def main(args: Array[String]): Unit = {
    println("========STARTED=========")

    val ls = List("Tech->BigData,Tool->Hive", "Tech->MS,Tool->Java")

    ls.foreach(println)

    println

    val tech = ls.flatMap(x => x.split(",")).filter(x => x.contains("Tech"))
      .map(x => x.replace("Tech", "Technology"))

    tech.foreach(println)

    println

    val tool = ls.flatMap(x => x.split(",")).filter(x => x.contains("Tool"))
      .map(x => x.replace("Tool", "Tools"))

    tool.foreach(println)

    println

  }

}

