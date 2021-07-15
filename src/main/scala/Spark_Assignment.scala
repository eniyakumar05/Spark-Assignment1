import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Spark_Assignment extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  val Users = ("src/main/resources/users.csv")
  val df1 = spark.read.option("header",true).csv(Users)
  val Transactions = ("src/main/resources/Transactions.csv")
  val df2 = spark.read.option("header",true).csv(Transactions)
  df1.show()
  df2.show()
  df1.printSchema()
  df2.printSchema()

  df1.join(df2,df1("UserID")===df2("User_ID"),"inner").show()

  //count of unique location where each product is sold
  val sold = df1.join(df2,df1("UserID")===df2("User_ID"),"inner")
  sold.groupBy("Location").count().show()

  //Find out product bought by each user
  val Product_Bought = spark.read.option("header",true).csv(Transactions)
  Product_Bought.groupBy("Product_ID","User_ID").count.show()

 
  //Total spending done by each user on each product
  val Total_Spending = spark.read.option("header",true).csv(Transactions)
  Total_Spending.groupBy("User_ID","Product_ID").agg(sum("Price")).show(false)

}
