/**
  * @Author : wzdnzd
  * @Time : 2019-07-11
  * @Project : bigdata
  */

package com.wzdnzd.config

case class Product(pid: Int, name: String, categories: String, imgUrl: String, tags: String)

case class ProductRating(uid: Int, pid: Int, score: Double, timestamp: Long)

case class MongoConfig(uri: String, db: String)

case class Recommendation(pid: Int, score: Double)

case class UserRecs(uid: Int, recs: Seq[Recommendation])

case class ProductRecs(pid: Int, recs: Seq[Recommendation])

object ConstantValue {
	val PRODUCT_COLLECTION = "product"
	val RATING_COLLECTION = "rating"

	val RATE_MORE_PRODUCTS = "rate_more_products"
	val RATE_MORE_RECENTLY_PRODUCTS = "rate_more_recently_products"
	val AVERAGE_PRODUCTS = "average_products"
	val STREAM_RECS_COLLECTION = "stream_recs"
	val CONTENT_PRODUCT_RECS = "content_recs"
	val ITEM_CF_PRODUCT_RECS = "itemcf_recs"
	val USER_RECS = "user_recs"
	val PRODUCT_RECS = "product_recs"

	val MAX_RECOMMEND = 20
	val MAX_USER_RATINGS_NUM = 20
	val MAX_SIM_PRODUCTS_NUM = 20

	val CONFIG: Map[String, String] = Map(
		"spark.server" -> "local[*]",
		"mongo.uri" -> "mongodb://hadoop-namenode-01:27017/recommend",
		"mongo.db" -> "recommend",
		"kafka.topic" -> "recommend",
		"redis.server" -> "hadoop-datanode-01:26379,hadoop-datanode-02:26379,hadoop-datanode-03:26379",
		"redis.password" -> "redis",
		"redis.cluster" -> "mymaster"
	)
}
