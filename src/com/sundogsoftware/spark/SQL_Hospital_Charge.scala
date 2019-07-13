package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object SQL_Hospital_Charge {
  
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    //set context 
    
    val spark= SparkSession.builder().appName("SQL_Hospital_Charge").master("local[*]")
    .config("spark.sql.warehouse.dir","file:///C:/temp").getOrCreate()
    
    val df = spark.read.format("com.databricks.spark.csv")
    .option("header", "true").option("inferSchema", "true").load("../inpatientCharges.csv")
    
  
    df.registerTempTable("hospital")
    
    df.groupBy("ProviderState").count().show
    
  }
}