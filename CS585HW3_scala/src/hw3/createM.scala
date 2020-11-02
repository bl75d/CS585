package hw3
import java.io.FileWriter


import java.io.IOException

import java.util.Random

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.types.{StructType, StructField, StringType,IntegerType,FloatType};
import org.apache.spark.sql.functions._
//remove if not needed
import scala.collection.JavaConversions._
//create M1,M2 matrix data files
object CreateM extends App{
    class CreateM {
    
      var x: Int = _
    
      var y: Int = _
    
      var value: Int = _
    
      def create_data(a: Int, b: Int): Unit = {
        val r: Random = new Random()
        x = a
        y = b
        value = 1 + r.nextInt(20)
      }
    
    }

    val fw: FileWriter = new FileWriter("M1.txt")
    val fw2: FileWriter = new FileWriter("M2.txt")
    for (i <- 0.until(500); j <- 0.until(500)) {
      val M: CreateM = new CreateM()
      M.create_data(i, j)
      val dt: String = M.x + "," + M.y + "," + M.value
      fw.write(dt + "\n")
      
      val M2: CreateM = new CreateM()
      M2.create_data(i, j)
      val dt2: String = M.x + "," + M.y + "," + M.value

      fw2.write(dt + "\n")
    }
    fw.close()
    fw2.close()
  

}


