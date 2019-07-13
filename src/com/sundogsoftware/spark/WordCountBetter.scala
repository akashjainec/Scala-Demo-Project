package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word occurs in a book, using regular expressions. */
object WordCountBetter {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCountBetter")   
    
    // Load each line of my book into an RDD
    val input = sc.textFile("../book.txt")
    
    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))
    
    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    val abcd = lowercaseWords.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    
    val sortedCountAbc = abcd.map( x => (x._2, x._1)).sortByKey()
    
   // val wordCountsSorted = abcd.map( x => (x._2, x._1) ).sortByKey()
    
    
    // Count of the occurrences of each word
    //val wordCounts = lowercaseWords.countByValue()
    
    // Print the results
     for (result <- sortedCountAbc) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
  }
  
}

