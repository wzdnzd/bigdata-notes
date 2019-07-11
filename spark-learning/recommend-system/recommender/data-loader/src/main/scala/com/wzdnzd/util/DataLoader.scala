/**
  * @Author : wzdnzd
  * @Time : 2019-07-10
  * @Project : bigdata
  */

package com.wzdnzd.util

import java.net.URL

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


case class Product(pid: Int, name: String, categories: String, imgUrl: String, tags: String)

case class Rating(uid: Int, pid: Int, score: Double, timestamp: Long)

case class MongoConfig(uri: String, db: String)

object DataLoader {
	val BASEDIR: URL = DataLoader.getClass.getResource("/data")
	val PRODUCT_DATA_PATH: String = BASEDIR + "/products.csv"
	val RATING_DATA_PATH: String = BASEDIR + "/ratings.csv"

	val MONGODB_PRODUCT_COLLECTION = "product"
	val MONGODB_RATING_COLLECTION = "rating"

	def main(args: Array[String]): Unit = {
		val config = Map(
			"spark.server" -> "local[*]",
			"mongo.uri" -> "mongodb://hadoop-namenode-01:27017/recommend",
			"mongo.db" -> "recommend"
		)

		val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.server")).setAppName("DataLoader")
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
			Rating(contents(0).toInt, contents(1).toInt, contents(2).toDouble, contents(3).toLong)
		}).toDF()

		implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

		storeToMongo(productDF, MONGODB_PRODUCT_COLLECTION)("pid")
		storeToMongo(ratingDF, MONGODB_RATING_COLLECTION)("pid", "uid")
	}

	def storeToMongo(df: DataFrame, collectionName: String)(args: String*)(implicit mongoConfig: MongoConfig): Unit = {
		val mongoClient: MongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
		val collection = mongoClient(mongoConfig.db)(collectionName)
		collection.dropCollection()
		df.write
			.option("uri", mongoConfig.uri)
			.option("collection", collectionName)
			.mode("overwrite")
			.format("com.mongodb.spark.sql")
			.save()

		args.foreach(arg => collection.createIndex(MongoDBObject(arg -> 1)))

		mongoClient.close()
	}
}
