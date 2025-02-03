package sparkDemo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types._
object dfpractice {

  object obj {
    def main(args: Array[String]): Unit = {
      println("===Hello====")
      val conf = new SparkConf().setAppName("first").setMaster("local[*]").
        set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")

      val spark = SparkSession.builder().appName("RDD to DataFrame")
        .master("local").getOrCreate()

      import spark.implicits._

      val data = Seq(
        (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
        (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
        (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
        (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
        (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
        (5, "02-14-2011", 200.0, "Gymnastics", null, "cash"),
        (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
        (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
        (8, "02-14-2011", 200.0, "Gymnastics", null, "cash"))

      val df = data.toDF("id", "tdate", "amount", "category", "product", "spendby")
      df.show()

      val data2 = Seq(
        (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
        (5, "02-14-2011", 200.0, "Gymnastics", null, "cash"),
        (6, "02-14-2011", 200.0, "Winter", null, "cash"),
        (7, "02-14-2011", 200.0, "Winter", null, "cash"))

      val df1 = data2.toDF("id", "tdate", "amount", "category", "product", "spendby")
      df1.show()

      val data4 = Seq(
        (1, "raj"),
        (2, "ravi"),
        (3, "sai"),
        (5, "rani"))

      val cust = data4.toDF("id", "name")
      cust.show()

      val data3 = Seq(
        (1, "raj"),
        (3, "mobile"),
        (7, "laptop"))

      val prod = data3.toDF("id", "product")
      prod.show()

      df.createOrReplaceTempView("df")
      df1.createOrReplaceTempView("df1")
      cust.createOrReplaceTempView("cust")
      prod.createOrReplaceTempView("prod")

      spark.sql("select * from df ").show()
      spark.sql("select id,tdate from df order by id").show()
      spark.sql("select id,tdate,category from df where category='Exercise'order by id").show()

      spark.sql("select id,tdate,category,spendby from df where category='Exercise' and spendby='cash' ").show()
      spark.sql("select id,tdate from df where category='Exercise' and spendby='cash' ").show()
      spark.sql("select id,tdate from df where category='Exercise' and spendby='cash' ").show()
      spark.sql("select * from df where category in ('Exercise','Gymnastics')").show()
      spark.sql("select * from df where product like ('%Gymnastics%')").show()
      spark.sql("select * from df where category != 'Exercise'").show()
      spark.sql("select * from df where category not in ('Exercise','Gymnastics')").show()
      spark.sql("select * from df where product is null").show()
      spark.sql("select max(id) from df ").show()
      spark.sql("select min(id) from df ").show()
      spark.sql("select count(1) from df ").show()
      spark.sql("select *,case when spendby='cash' then 1 else 0 end as status from df ").show()
      spark.sql("select concat(id,'-',category) as concat from df ").show()
      spark.sql("select concat_ws('-',id,category,product) as concat from df ").show()
      spark.sql("select category,lower(category) as lower from df ").show()
      spark.sql("select amount,ceil(amount) from df").show()
      spark.sql("select amount,round(amount) from df").show()
      spark.sql("select coalesce(product,'NA') from df").show()
      spark.sql("select trim(product) from df").show()
      spark.sql("select distinct category,spendby from df").show()
      spark.sql("select substring(trim(product),1,10) from df").show()
      spark.sql("select SUBSTRING_INDEX(category,' ',1) as spl from df").show()
      spark.sql("select * from df union all select * from df1").show()
      spark.sql("select * from df union select * from df1 order by id").show()
      spark.sql("select category, sum(amount) as total from df group by category").show()
      spark.sql("select category,spendby,sum(amount) as total from df group by category,spendby").show()
      spark.sql("select category,spendby,sum(amount) As total,count(amount) as cnt from df group by category,spendby").show()
      spark.sql("select category, max(amount) as max from df group by category").show()
      spark.sql("select category, max(amount) as max from df group by category order by category").show()
      spark.sql("select category, max(amount) as max from df group by category order by category desc").show()
      spark.sql("SELECT category,amount, row_number() OVER ( partition by category order by amount desc ) AS row_number FROM df").show()
      spark.sql("SELECT category,amount, dense_rank() OVER ( partition by category order by amount desc ) AS dense_rank FROM df").show()
      spark.sql("SELECT category,amount, rank() OVER ( partition by category order by amount desc ) AS rank FROM df").show()
      spark.sql("SELECT category,amount, lead(amount) OVER ( partition by category order by amount desc ) AS lead FROM df").show()
      spark.sql("SELECT category,amount, lag(amount) OVER ( partition by category order by amount desc ) AS lag FROM df").show()
      spark.sql("select category,count(category) as cnt from df group by category having count(category)>1").show()
      spark.sql("select a.id,a.name,b.product from cust a join prod b on a.id=b.id").show()
      spark.sql("select a.id,a.name,b.product from cust a left join prod b on a.id=b.id").show()
      spark.sql("select a.id,a.name,b.product from cust a right join prod b on a.id=b.id").show()
      spark.sql("select a.id,a.name,b.product from cust a full join prod b on a.id=b.id").show()
      spark.sql("select a.id,a.name from cust a LEFT ANTI JOIN prod b on a.id=b.id").show()
      spark.sql("select a.id,a.name from cust a LEFT SEMI JOIN prod b on a.id=b.id").show()
      spark.sql("select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date from df").show()
      spark.sql("""
select sum(amount) as total , con_date from(
select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date,amount,category,product,spendby from df)
group by con_date""").show()

    }
  }
}