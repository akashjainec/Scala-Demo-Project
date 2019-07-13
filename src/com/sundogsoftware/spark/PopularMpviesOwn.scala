package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object PopularMpviesOwn {
  
  def main (args : Array[String]) {
    
   /* Define log */
   Logger.getLogger("org").setLevel(Level.ERROR)
    
   val sc = new SparkContext("local[*]","PopularMpviesOwn")
   
   val lines = sc.textFile("../ratings.dat")
   
   val movies = lines.map(s => (s.split("::")(1).toInt, 1))
   
   val count = movies.reduceByKey((x, y) => x + y)
   
   val flip = count.map(x => (x._2, x._1))
   val sorted = flip.sortByKey()
   val result = sorted.collect()
   
   result.foreach(println)
   
  }
}