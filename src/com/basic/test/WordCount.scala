package com.basic.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dell-pc on 2016/4/5.
  */
object WordCount {

  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("mapFunction").setMaster("local")
    val sc=new SparkContext(conf)
    val textFile=sc.textFile("E://TDDOWNLOAD//BigDataSpark//spark-1.6.0-bin-hadoop2.6//README.md")
    val wordCount=textFile.flatMap(lines => lines.split(" ")).map(word => (word,1)).reduceByKey((a,b) => a+b)
    //wordCount.foreach(wordNumberPair=> {println(wordNumberPair._1+"'"+wordNumberPair._2)})
    val sortwordCount=wordCount.sortByKey(true)
    val testword=wordCount.sortBy(word=> word._2,true)
    testword.foreach(wordNumberPair=> {println(wordNumberPair._1+"'"+wordNumberPair._2)})
    //sortwordCount.foreach(wordNumberPair=> {println(wordNumberPair._1+"'"+wordNumberPair._2)})
  }
}
