import org.apache.spark.{SparkContext, SparkConf}

val conf=new SparkConf().setAppName("mapFunction").setMaster("local")
val sc=new SparkContext(conf)
val rdd=sc.makeRDD(1 to 5 ,1)

val rdd_map=rdd.map(x => x.toFloat)
