/**
  * @Author : wzdnzd
  * @Time :  2019-07-13
  * @Project : bigdata
  */

package com.wzdnzd.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import com.wzdnzd.config.{ConstantValue, MongoConfig, ProductRecs}
import com.wzdnzd.streaming.utils.SentinelJedisPool
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object ConnHelper extends Serializable {
	lazy val mongoClient = MongoClient(MongoClientURI(ConstantValue.CONFIG("mongo.uri")))
	lazy val jedis: Jedis = SentinelJedisPool.getJedis()
}


object StreamingRecommend {
	def main(args: Array[String]): Unit = {
		val kafkaParam = Map(
			"bootstrap.servers" -> "hadoop-datanode-01:9092,hadoop-datanode-02:9092,hadoop-datanode-03:9092",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "recommend",
			"auto.offset.reset" -> "latest"
		)

		val sparkConf = new SparkConf().setMaster(ConstantValue.CONFIG("spark.server")).setAppName("OnlineRecommend")
		val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

		val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(2))

		implicit val mongoConfig: MongoConfig = MongoConfig(ConstantValue.CONFIG("mongo.uri"), ConstantValue.CONFIG("mongo.db"))

		import sparkSession.implicits._

		val productsMatrix = sparkSession
			.read
			.option("uri", mongoConfig.uri)
			.option("collection", ConstantValue.PRODUCT_RECS)
			.format("com.mongodb.spark.sql")
			.load
			.as[ProductRecs]
			.rdd
			.map { item => (item.pid, item.recs.map(x => (x.pid, x.score)).toMap) }.collectAsMap()
		val productsMatrixBroadCast = sparkSession.sparkContext.broadcast(productsMatrix)

		val kafkaStream = KafkaUtils.createDirectStream[String, String](streamingContext, LocationStrategies.PreferConsistent,
			ConsumerStrategies.Subscribe[String, String](Array(ConstantValue.CONFIG("kafka.topic")), kafkaParam))

		val ratingStream = kafkaStream.map { msg =>
			val contents = msg.value().split("\\|")
			(contents(0).toInt, contents(1).toInt, contents(2).toDouble, contents(3).toLong)
		}

		ratingStream.foreachRDD {
			rdd =>
				rdd.foreach {
					case (uid, pid, _, _) =>
						val userRecentlyRatings = getUserRecentlyRating(ConstantValue.MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)
						val simProducts = getTopNSimProducts(ConstantValue.MAX_SIM_PRODUCTS_NUM, pid, uid, productsMatrixBroadCast.value)

						val streamRecs = computeProductScores(productsMatrixBroadCast.value, userRecentlyRatings, simProducts)
						saveRecsToMongoDB(uid, streamRecs)

				}
		}

		streamingContext.start()

		println("streaming started")

		streamingContext.awaitTermination()
	}

	import collection.JavaConverters._

	def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
		jedis.lrange("uid:" + uid.toString, 0, num).asScala.map { item =>
			val contents = item.split("\\:")
			(contents(0).trim.toInt, contents(1).trim.toDouble)
		}.toArray
	}

	def getTopNSimProducts(num: Int, pid: Int, uid: Int,
	                       products: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
	                      (implicit mongoConfig: MongoConfig): Array[Int] = {

		val allSimProducts = products(pid).toArray
		val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(ConstantValue.RATING_COLLECTION)
			.find(MongoDBObject("uid" -> uid)).toArray.map { item =>
			item.get("pid").toString.toInt
		}
		allSimProducts.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2).take(num).map(x => x._1)
	}

	def computeProductScores(products: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]],
	                         recentlyRatings: Array[(Int, Double)], topNSimProducts: Array[Int]): Array[(Int, Double)] = {
		val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
		val record = scala.collection.mutable.HashMap[Int, (Int, Int)]()

		for (simProduct <- topNSimProducts; recentlyRating <- recentlyRatings) {
			val simScore = getProductsSimScore(products, recentlyRating._1, simProduct)
			if (simScore > 0.6) {
				scores += ((simProduct, simScore * recentlyRating._2))

				val tuple = record.getOrElse(simProduct, (0, 0))
				var count = 0

				if (recentlyRating._2 > 3) {
					count = tuple._1 + 1
					record.put(simProduct, (count, tuple._2))
				} else {
					count = tuple._2 + 1
					record.put(simProduct, (tuple._1, count))
				}
			}
		}

		scores.groupBy(_._1).map { case (pid, sims) =>
			val (increment, decrement) = record.get(pid) match {
				case Some(x) => (x._1, x._2)
				case None => (1, 1)
			}

			(pid, sims.map(_._2).sum / sims.length + math.log(math.max(increment, 1)) - math.log(math.max(decrement, 1)))
		}.toArray.sortWith(_._2 > _._2)
	}

	def getProductsSimScore(products: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]], userRatingProduct: Int, topSimProduct: Int): Double = {
		products.get(topSimProduct) match {
			case Some(similarity) => similarity.get(userRatingProduct) match {
				case Some(score) => score
				case None => 0.0
			}
			case None => 0.0
		}
	}

	def saveRecsToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
		val collection: MongoCollection = ConnHelper.mongoClient(mongoConfig.db)(ConstantValue.STREAM_RECS_COLLECTION)

		collection.findAndRemove(MongoDBObject("uid" -> uid))
		collection.insert(MongoDBObject("uid" -> uid, "recs" ->
			streamRecs.map(x => MongoDBObject("pid" -> x._1, "score" -> x._2))))
	}
}
