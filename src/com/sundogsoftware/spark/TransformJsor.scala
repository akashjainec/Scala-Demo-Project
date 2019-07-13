package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._

object TransformJsor {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark= SparkSession.builder()
              .appName("TransformJsor")
              .master("local[*]")
              .config("spark.sql.warehouse.dir", "file:///C:/temp").
              getOrCreate()
              
   def main(args: Array[String]){
    
    //import spark.implicits._
    
    val readjson = spark.read.json("../movies.json")
  }
  
}