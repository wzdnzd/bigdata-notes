/**
  * @Author : wzdnzd
  * @Time :  2019-07-14
  * @Project : bigdata
  */

package com.wzdnzd.itemcf

import com.wzdnzd.config.{ConstantValue, MongoConfig, ProductRating, ProductRecs, Recommendation}
import com.wzdnzd.utils.DataWriteUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ItemCFRecommend {
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setMaster(ConstantValue.CONFIG("spark.server")).setAppName("ItemCFRecommend")
		val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

		import sparkSession.implicits._
		implicit val mongoConfig: MongoConfig = MongoConfig(ConstantValue.CONFIG("mongo.uri"), ConstantValue.CONFIG("mongo.db"))

		val ratingDF = sparkSession.read
			.option("uri", mongoConfig.uri)
			.option("collection", ConstantValue.RATING_COLLECTION)
			.format("com.mongodb.spark.sql")
			.load()
			.as[ProductRating]
			.map(x => (x.uid, x.pid, x.score))
			.toDF("uid", "pid", "score")
			.cache()

		val productRatingCountDF = ratingDF.groupBy("pid").count()
		val ratingWithCountDF = ratingDF.join(productRatingCountDF, "pid")

		val joinedDF = ratingWithCountDF.join(ratingWithCountDF, "uid")
			.toDF("uid", "product1", "score1", "count1", "product2", "score2", "count2")
			.select("uid", "product1", "count1", "product2", "count2")
		joinedDF.createOrReplaceTempView("joined")

		val query = "select product1, product2, count(uid) as count, first(count1) as count1, first(count2) as count2 from joined group by product1, product2"
		val coOccurrenceDF = sparkSession.sql(query).cache()

		val simDF = coOccurrenceDF.map {
			row =>
				val similarity = row.getAs[Long]("count") / math.sqrt(row.getAs[Long]("count1") * row.getAs[Long]("count2"))
				(row.getInt(0), (row.getInt(1), similarity))
		}
			.rdd
			.groupByKey()
			.map {
				case (pid, recs) =>
					ProductRecs(pid, recs.toList
						.filter(x => x._1 != pid)
						.sortWith(_._2 > _._2)
						.take(ConstantValue.MAX_RECOMMEND)
						.map(x => Recommendation(x._1, x._2)))
			}
			.toDF()

		DataWriteUtil.storeToMongo(simDF, ConstantValue.ITEM_CF_PRODUCT_RECS)

		sparkSession.stop()
	}
}
