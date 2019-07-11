/**
  * @Author : wzdnzd
  * @Time : 2019-07-11
  * @Project : bigdata
  */

package com.wzdnzd.config

case class Product(pid: Int, name: String, categories: String, imgUrl: String, tags: String)

case class Rating(uid: Int, pid: Int, score: Double, timestamp: Long)

case class MongoConfig(uri: String, db: String)

object ConstantValue {
	val MONGODB_PRODUCT_COLLECTION = "product"
	val MONGODB_RATING_COLLECTION = "rating"

	val CONFIG: Map[String, String] = Map(
		"spark.server" -> "local[*]",
		"mongo.uri" -> "mongodb://hadoop-namenode-01:27017/recommend",
		"mongo.db" -> "recommend"
	)
}
