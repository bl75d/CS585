package hw3
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.types.{StructType, StructField, StringType,IntegerType,FloatType};
import org.apache.spark.sql.functions._

object p4 extends App{
     val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()
    
      val Tschema = new StructType(Array(StructField("x",IntegerType,true),
      StructField("y",IntegerType,true),
      StructField("value",IntegerType,true),
      ))

    spark.sparkContext.setLogLevel("ERROR")
    val df1 = spark.read.option("header", "false").schema(Tschema).csv("./data/M1.txt")
    val df2 = spark.read.option("header", "false").schema(Tschema).csv("./data/M2.txt")
    
    //map 2 dataframes
    val df3=df1.select(col("y").as("key"),lit("FlagM").as("Flag"),col("x") as("i"),col("value").as("Mvalue")).toDF()
    df3.show(false)
    
    val df4=df2.select(col("x").as("key"),lit("FlagN").as("Flag"),col("y") as("k"),col("value").as("Nvalue")).toDF()
    df4.show(false)
    
    //reduce
     val df5=df3.join(df4,("key")).select(col("i"),col("k"),col("Mvalue").multiply(col("Nvalue")).as("v")).toDF()
     df5.show(false)
     
     val df6=df5.groupBy("i", "k").agg(sum("v").as("T")).toDF()
     df6.show(false)
     df6.coalesce(1).write.csv("./data/output.txt")


}