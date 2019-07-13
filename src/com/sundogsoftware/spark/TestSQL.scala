package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._

object TestSQL {
  
  def main(args: Array[String]){
    //define logger
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    //define session
    
    val dwdir = "file:///C:/temp"
    val spark = SparkSession
    .builder().appName("TestSQL").master("local[*]")
    .config("spark.sql.warehouse.dir",dwdir)
    .getOrCreate()
    
  }
}