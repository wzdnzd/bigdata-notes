/**
  * @Author : wzdnzd
  * @Time :  2019-07-13
  * @Project : bigdata
  */

package com.wzdnzd.offline

import breeze.numerics.sqrt
import com.wzdnzd.config.{ConstantValue, MongoConfig, ProductRating}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("ALSTrainer").setMaster(ConstantValue.CONFIG("spark.server"))
		val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

		val mongoConfig = MongoConfig(ConstantValue.CONFIG("mongo.uri"), ConstantValue.CONFIG("mongo.db"))

		import sparkSession.implicits._

		val ratingRDD = sparkSession
			.read
			.option("uri", mongoConfig.uri)
			.option("collection", ConstantValue.RATING_COLLECTION)
			.format("com.mongodb.spark.sql")
			.load()
			.as[ProductRating]
			.rdd
			.map(rating => Rating(rating.uid, rating.pid, rating.score))
			.cache()

		val dataset = ratingRDD.randomSplit(Array(0.8, 0.2))
		val (trainData, valData) = (dataset(0), dataset(1))

		adjustParams(trainData, valData)

		sparkSession.stop()

	}

	def adjustParams(trainData: RDD[Rating], valData: RDD[Rating]): Unit = {
		val result = for (rank <- 10 to 50 by 10; lambda <- Array(0.01, 0.1, 0.5, 1))
			yield {
				val model = ALS.train(trainData, rank, 20, lambda)
				val rmse = computeRMSE(model, valData)
				(rank, lambda, rmse)
			}

		println(result.minBy(_._3))
	}

	def computeRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
		val userProducts = data.map(item => (item.user, item.product))
		val predictRDD = model.predict(userProducts)

		val realRating = data.map(item => ((item.user, item.product), item.rating))
		val predictRating = predictRDD.map(item => ((item.user, item.product), item.rating))

		sqrt(
			realRating.join(predictRating).map {
				case ((_, _), (real, predict)) =>
					val error = real - predict
					error * error
			}.mean()
		)
	}
}
