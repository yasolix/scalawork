
// http://stackoverflow.com/questions/33587757/creating-map-values-in-spark-using-scala



//SparkContext.parallelize transforms from Seq[T] to RDD[T]. If you want to create RDD[(String, String)] where each element is an individual key-value pair from the original Map use:

import org.apache.spark.rdd.RDD

val m = Map("red" -> "#FF0000","azure" -> "#F0FFFF","peru" -> "#CD853F")
val rdd: RDD[(String, String)] = sc.parallelize(m.toSeq)

//If you want RDD[Map[String,String]] (not that it makes any sense with a single element) use:

//val rdd: RDD[Map[String,String]] = sc.parallelize(Seq(m))