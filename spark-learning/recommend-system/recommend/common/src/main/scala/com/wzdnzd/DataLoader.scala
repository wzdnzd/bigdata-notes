/**
  * @Author : wzdnzd
  * @Time : 2019-07-11
  * @Project : bigdata
  */

package com.wzdnzd

import java.net.URL

import com.wzdnzd.config.{ConstantValue, MongoConfig, Product, ProductRating}
import com.wzdnzd.utils.DataWriteUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataLoader {
	val BASEDIR: URL = DataLoader.getClass.getResource("/data")
	val PRODUCT_DATA_PATH: String = BASEDIR + "/products.csv"
	val RATING_DATA_PATH: String = BASEDIR + "/ratings.csv"

	def main(args: Array[String]): Unit = {
		val sparkConf: SparkConf = new SparkConf().setMaster(ConstantValue.CONFIG("spark.server")).setAppName("DataLoader")
		val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

		import sparkSession.implicits._

		val productRDD = sparkSession.sparkContext.textFile(PRODUCT_DATA_PATH)
		val productDF = productRDD.map(item => {
			val contents = item.split("\\^")
			Product(contents(0).toInt, contents(1).trim, contents(4).trim, contents(5).trim, contents(6).trim)
		}).toDF()

		val ratingRDD = sparkSession.sparkContext.textFile(RATING_DATA_PATH)
		val ratingDF = ratingRDD.map(item => {
			val contents = item.split(",")
			ProductRating(contents(0).toInt, contents(1).toInt, contents(2).toDouble, contents(3).toLong)
		}).toDF()

		implicit val mongoConfig: MongoConfig = MongoConfig(ConstantValue.CONFIG("mongo.uri"), ConstantValue.CONFIG("mongo.db"))

		DataWriteUtil.storeToMongo(productDF, ConstantValue.PRODUCT_COLLECTION, "pid")
		DataWriteUtil.storeToMongo(ratingDF, ConstantValue.RATING_COLLECTION, "pid", "uid")

		println("+++++ data load finished +++++")
	}
}
