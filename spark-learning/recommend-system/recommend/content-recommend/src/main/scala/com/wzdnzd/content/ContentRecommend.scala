/**
  * @Author : wzdnzd
  * @Time :  2019-07-14
  * @Project : bigdata
  */

package com.wzdnzd.content

import com.wzdnzd.config.{ConstantValue, MongoConfig, ProductRecs, Recommendation}
import com.wzdnzd.utils.DataWriteUtil
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

case class Product(pid: Int, name: String, imageUrl: String, categories: String, tags: String)

object ContentRecommend {
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setMaster(ConstantValue.CONFIG("spark.server")).setAppName("ContentRecommend")
		val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

		import sparkSession.implicits._
		implicit val mongoConfig: MongoConfig = MongoConfig(ConstantValue.CONFIG("mongo.uri"), ConstantValue.CONFIG("mongo.db"))

		val productTagsDF = sparkSession.read
			.option("uri", mongoConfig.uri)
			.option("collection", ConstantValue.PRODUCT_COLLECTION)
			.format("com.mongodb.spark.sql")
			.load()
			.as[Product]
			.map(
				x => (x.pid, x.name, x.tags.map(c => if (c == '|') ' ' else c))
			)
			.toDF("pid", "name", "tags")
			.cache()

		val tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")
		val wordsDataDF = tokenizer.transform(productTagsDF)

		val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(800)
		val dataDF = hashingTF.transform(wordsDataDF)

		val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
		val idfModel = idf.fit(dataDF)
		val rescaledDataDF = idfModel.transform(dataDF)

		val productFeatures = rescaledDataDF.map {
			row => (row.getAs[Int]("pid"), row.getAs[SparseVector]("features").toArray)
		}
			.rdd
			.map {
				case (productId, features) => (productId, new DoubleMatrix(features))
			}

		val productRecs = productFeatures.cartesian(productFeatures)
			.filter {
				case (a, b) => a._1 != b._1
			}
			.map {
				case (a, b) =>
					val score = a._2.dot(b._2) / (a._2.norm2() * b._2.norm2())
					(a._1, (b._1, score))
			}
			.filter(_._2._2 > 0.4)
			.groupByKey()
			.map {
				case (pid, recs) =>
					ProductRecs(pid, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
			}
			.toDF()

		DataWriteUtil.storeToMongo(productRecs, ConstantValue.CONTENT_PRODUCT_RECS)

		sparkSession.stop()
	}
}
