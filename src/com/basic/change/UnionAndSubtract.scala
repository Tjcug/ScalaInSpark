package com.basic.change

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by dell-pc on 2016/3/25.
  */
object UnionAndSubtract {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("mapFunction").setMaster("local")
    val sc=new SparkContext(conf)

    val rdd1=sc.makeRDD(1 to 3,1)
    val rdd2=sc.makeRDD(2 to 4,1)
    rdd2.filter(_>3)
    val unionRDD=rdd1.union(rdd2)  //对RDD进行并集操作
    val intersectionRDD=rdd1.intersection(rdd2)//对RDD进行交集操作，并且交集中不会出现相同元素
    val subtractRDD=rdd1.subtract(rdd2)//对RDD进行相减操作 就是存在在rdd1中，而不存在rdd2中的元素

    for(i <- unionRDD.collect())
      println(i+ " ")
    for(i <- intersectionRDD.collect())
      println(i+ " ")
    for(i <- subtractRDD.collect())
      println(i+ " ")
  }
}
