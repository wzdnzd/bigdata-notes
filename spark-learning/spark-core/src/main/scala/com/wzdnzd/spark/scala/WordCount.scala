/**
  * @Author : wzdnzd
  * @Time :  2019-07-04
  * @Project : bigdata
  */

package com.wzdnzd.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
	def main(args: Array[String]): Unit = {
		val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
		val sc: SparkContext = new SparkContext(sparkConf)

		// fix java.io.IOException: No FileSystem for scheme : hdfs
		sc.hadoopConfiguration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
		sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

		// val dataPath = WordCount.getClass.getResource("/").getPath + "/data/text1"
		val dataPath = "hdfs://hadoop-namenode-01:9000/learn/data/wordcount"

		sc.textFile(dataPath).flatMap(line => line.toLowerCase().replaceAll("[^a-z0-9A-Z\\-]", " ")
			.split(" ")).filter(word => !"".equals(word.strip())).map(word => (word, 1)).reduceByKey(_ + _)
			.sortBy(_._2, ascending = false).collect().take(50).foreach(println)

		sc.stop()
	}
}
