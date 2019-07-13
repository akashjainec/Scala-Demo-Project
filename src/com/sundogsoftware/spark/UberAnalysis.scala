package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object UberAnalysis {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //defining spark session
  
  val spark = SparkSession
              .builder()
              .appName("UberAnalysis")
              .config("spark.sql.warehouse.dir", "file:///C:/temp")
              .master("local[*]")
              .getOrCreate()
              
 val dataset = spark.sparkContext.textFile("../uber.csv")
 val header = dataset.first()
 val format = new java.text.SimpleDateFormat("MM/dd/yyyy")
 val days = Array("Sun","Mon","Tue","Wed","Thu","Fri","Sat","Sun")
 val eliminate = dataset.filter(x => x != header)
 val split = eliminate.map(x => x.split(",")).map(x => (x(0), format.parse(x(1)), x(3)))
 //val combine = split.map(x => )
}