/**
  * @Author : wzdnzd
  * @Time : 2019-07-12
  * @Project : bigdata
  */

package com.wzdnzd.statistics

import java.text.SimpleDateFormat
import java.util.Date

import com.wzdnzd.config.{ConstantValue, MongoConfig, ProductRating}
import com.wzdnzd.utils.DataWriteUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StatisticsRecommend {
	def main(args: Array[String]): Unit = {
		val sparkConf: SparkConf = new SparkConf().setMaster(ConstantValue.CONFIG("spark.server")).setAppName("StatisticsRecommend")
		val sparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate()

		implicit val mongoConfig: MongoConfig = MongoConfig(ConstantValue.CONFIG("mongo.uri"), ConstantValue.CONFIG("mongo.db"))

		import sparkSession.implicits._

		val df = sparkSession
			.read
			.option("uri", mongoConfig.uri)
			.option("collection", ConstantValue.RATING_COLLECTION)
			.format("com.mongodb.spark.sql")
			.load()
			.as[ProductRating]
			.toDF()

		var tempView: String = "ratings"

		df.createOrReplaceTempView(tempView)

		// historical hot items
		var query: String = "select pid, count(pid) as count from " + tempView + " group by pid order by count desc"
		var hotProduct = sparkSession.sql(query)
		DataWriteUtil.storeToMongo(hotProduct, ConstantValue.RATE_MORE_PRODUCTS)

		// average score
		query = "select pid, avg(score) as avg_score from " + tempView + " group by pid order by avg_score desc"
		DataWriteUtil.storeToMongo(sparkSession.sql(query), ConstantValue.AVERAGE_PRODUCTS)

		// recently hot items
		val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMM")
		sparkSession.udf.register("format", (x: Long) => dateFormat.format(new Date(x * 1000L)).toLong)
		query = "select pid, score, format(timestamp) as date from " + tempView
		hotProduct = sparkSession.sql(query)

		tempView = "rating_month"

		hotProduct.createOrReplaceTempView(tempView)
		query = "select pid, count(pid) as count, date from " + tempView + " group by date, pid order by date desc, count desc"
		hotProduct = sparkSession.sql(query)
		DataWriteUtil.storeToMongo(hotProduct, ConstantValue.RATE_MORE_RECENTLY_PRODUCTS)

		sparkSession.stop()
	}
}
