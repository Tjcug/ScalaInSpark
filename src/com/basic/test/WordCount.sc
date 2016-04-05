import org.apache.spark.{SparkContext, SparkConf}

val conf=new SparkConf().setAppName("mapFunction").setMaster("local")
val sc=new SparkContext(conf)
val textFile=sc.textFile("E:\\TDDOWNLOAD\\BigDataSpark\\spark-1.6.0-bin-hadoop2.6\\READEME.md")
val wordCount=textFile.flatMap(lines => lines.split(" ")).map(word => (word,1)).reduceByKey((a,b) => a+b)
wordCount.collect()
val sortwordCount=wordCount.sortByKey(true)
//val testword=wordCount.sortBy((a,b)=> b,)
sortwordCount.collect()
