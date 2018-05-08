package Initialisation

import org.apache.spark.{SparkConf, SparkContext}

object Spark {
  val conf = new SparkConf()
    .setAppName("C4.5")
 .setMaster("local[*]")
   // .set("spark.executor.memory","20g")
  //  .set("spark.executor.memory","8g")
  val sc = new SparkContext(conf)
}
