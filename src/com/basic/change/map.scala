package com.basic.change

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by dell-pc on 2016/3/25.
  */
object map {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("mapFunction").setMaster("local")
    val sc=new SparkContext(conf)
    val rdd=sc.makeRDD(1 to 5 ,1)

    val rdd_map=rdd.map(x => x.toFloat)   //map函数将RDD中类型为T的元素一对一映射为类型为U的元素
    val flatMapRdd=rdd.flatMap(x => 1 to x)//flatMap函数将RDD中的每一个元素进行一对多的转换
    for(i <- flatMapRdd.collect())
      println(i)

    val distinctRdd=flatMapRdd.distinct() //distinct函数 返回所有RDD中不一样的元素
    for( i <- distinctRdd.collect())
      println(i)

  }
}
