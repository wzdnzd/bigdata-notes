/**
  * @Author : wzdnzd
  * @Time :  2019-07-13
  * @Project : bigdata
  */

package com.wzdnzd.offline

import com.wzdnzd.config.{ConstantValue, MongoConfig, ProductRating, ProductRecs, Recommendation, UserRecs}
import com.wzdnzd.utils.DataWriteUtil
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

object OfflineRecommend {
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setMaster(ConstantValue.CONFIG("spark.server")).setAppName("OfflineRecommend")
		val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
		implicit val mongoConfig: MongoConfig = MongoConfig(ConstantValue.CONFIG("mongo.uri"), ConstantValue.CONFIG("mongo.db"))

		import sparkSession.implicits._

		val ratingRDD = sparkSession
			.read
			.option("uri", mongoConfig.uri)
			.option("collection", ConstantValue.RATING_COLLECTION)
			.format("com.mongodb.spark.sql")
			.load
			.as[ProductRating]
			.rdd
			.map(item => (item.pid, item.uid, item.score))
			.cache()

		val userRDD = ratingRDD.map(_._1).distinct()
		val productRDD = ratingRDD.map(_._2).distinct()

		val trainRDD = ratingRDD.map(item => Rating(item._1, item._2, item._3))

		val (rank, iteration, lambda) = (10, 20, 0.01)
		val model = ALS.train(trainRDD, rank, iteration, lambda)

		val userProducts = userRDD.cartesian(productRDD)
		val preRating = model.predict(userProducts)

		var df = preRating.filter(item => item.rating >= 0)
			.map(item => (item.user, (item.product, item.rating)))
			.groupByKey()
			.map {
				case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 >
					_._2).take(ConstantValue.MAX_RECOMMEND).map(x => Recommendation(x._1, x._2)))
			}
			.toDF()

		DataWriteUtil.storeToMongo(df, ConstantValue.USER_RECS)

		val productFeatures = model.productFeatures.map {
			case (pid, features) =>
				(pid, new DoubleMatrix(features))
		}
		df = productFeatures.cartesian(productFeatures).filter { case (v1, v2) => v1._1 != v2._1 }
			.map { case (v1, v2) =>
				val score = v1._2.dot(v2._2) / (v1._2.norm2() * v2._2.norm2())
				(v1._1, (v2._1, score))
			}
			.filter(_._2._2 >= 0.7)
			.groupByKey()
			.map {
				case (pid, products) => ProductRecs(pid, products.toList.map(x => Recommendation(x._1, x._2)))
			}.toDF()

		DataWriteUtil.storeToMongo(df, ConstantValue.PRODUCT_RECS)

		sparkSession.stop()
	}

}
