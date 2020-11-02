package hw3

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.types.{StructType, StructField, StringType,IntegerType,FloatType};
import org.apache.spark.sql.functions._
object p3 extends App{
    val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()
    
      val Tschema = new StructType(Array(StructField("TransID",IntegerType,true),
      StructField("CustID",IntegerType,true),
      StructField("TransTotal",FloatType,true),
      StructField("TransNumItems",IntegerType,true),
      StructField("TrnasDesc",StringType,true),
      ))

    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read.option("header", "false").schema(Tschema).csv("./data/Transaction.txt")

    //(1)
    val df1=df.filter(df("TransTotal") <200)
//    df1.show(false)
    
    //(2)
    val df2=df1.groupBy("TransNumItems")
    .agg(sum("TransTotal") as "Sum",avg("TransTotal") as "Avg",min("TransTotal") as "Min",max("TransTotal") as "Max")
    
    //(3)
    df2.show(false)
    
    //(4)
    val df3=df1.groupBy("CustID").agg(count("TransID") as "cnt3")
//    df3.show(false)
    
    //(5)
    val df4 =df.filter(df("TransTotal")<600)
//    df4.show(false)
   
    //(6)
    val df5=df4.groupBy("CustID").agg(count("TransID") as "cnt5")
//    df5.show(false)
    
    //(7)
      val df6=df5.select(col("CustID"), col("cnt5").multiply(5).as("count*3"))
      val df7=df6.join(df3,"CustID").filter(col("count*3").lt(col("cnt3")))
     
     //(8)
      df7.show(false);


}