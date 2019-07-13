package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._


object SensorDataAnalysis {
  
  //define logger
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  case class hvac_cls(date:String, time:String, targettemp:Int, actualtemp:Int, System:Int, SystemAge:Int, BuildingId:Int )
  case class building(BuildingID: Int,BuildingMgr: String,BuildingAge: Int,HVACproduct: String,Country: String)
  def main(args: Array[String]){
    
    val spark = SparkSession.builder()
    .appName("SensorDataAnalysis")
    .config("spark.sql.warehouse.dir", "file:///C:/temp")
    .master("local[*]")
    .getOrCreate()
    val data = spark.sparkContext.textFile("../HVAC.csv")
    val header = data.first()
    val data1 = data.filter(row => row != header)
    
 
    import spark.implicits._
    
    val mappeddata = data1.map(c => c.split(",")).map(x => hvac_cls(x(0), x(1), x(2).toInt, x(3).toInt, x(4).toInt, x(5).toInt, x(6).toInt)).toDF()
    mappeddata.registerTempTable("HVAC")
    val dataset = spark.sqlContext.sql("select *, IF((targettemp - actualtemp) > 5, '1', IF((targettemp - actualtemp) < -5, '1', 0)) AS tempchange from HVAC")
    dataset.registerTempTable("abc")
    
    val rdd_test = spark.sparkContext.textFile("../building.csv")
    val rdd_filter = rdd_test.first()
    val rdd_data = rdd_test.filter(c => c != rdd_filter)
    val mapped_date = rdd_data.map(x => x.split(",")).map(x => building(x(0).toInt, x(1), x(2).toInt, x(3), x(4))).toDF()
    mapped_date.registerTempTable("building")
    
    val cba = spark.sql("select h.*, b.BuildingAge, b.Country from abc h join building b on h.BuildingId = b.BuildingId")
    cba.write.parquet("../abcd.parquet")
    
    

    
    
    
    }
}