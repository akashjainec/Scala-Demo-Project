package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object PopularMoviesOwnNicer {
  
  def loadMovies() : Map[Int, String] = {
    
    // Handle character coding issues.
    
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    //Create a map of int to string 
    var moviesname: Map[Int, String] = Map()
    
    val lines = Source.fromFile("../ml-100k/u.item").getLines()
    for (line <- lines){
      var fields = line.split('|')
      if (fields.length > 1){
        moviesname += (fields(0).toInt -> fields(1))
      }
      }
    return moviesname
    }
  
  def main (args : Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("Local[*]", "PopularMoviesOwnNicer")
    
    //broadcast variable
    
    var nameDict = sc.broadcast(loadMovies)
    
   // Load the files
    
    val fileload = sc.textFile("../ml-100k/u.data")
    
    // movies load
    
    val movies = fileload.map(s => (s.split("\t")(1).toInt, 1))
    
    val moviescount = movies.reduceByKey((x, y) => x + y)
    
    val flipped = moviescount.map(s => (s._2, s._1))
    //sort 
    val sort = flipped.sortByKey()
    // sort with movies name
    val sortwithname = sort.map(s => (nameDict.value(s._2), s._1))
    
    val result = sortwithname.collect()
    
    result.foreach(println)
  }
    
}   

