/**
  * @Author : wzdnzd
  * @Time : 2019-07-11
  * @Project : bigdata
  */

package com.wzdnzd.utils

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.wzdnzd.config.MongoConfig
import org.apache.spark.sql.DataFrame

object DataWriteUtil {
	def storeToMongo(df: DataFrame, collectionName: String, indexes: String*)(implicit mongoConfig: MongoConfig): Unit = {
		val mongoClient: MongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
		val collection = mongoClient(mongoConfig.db)(collectionName)
		collection.dropCollection()
		df.write
			.option("uri", mongoConfig.uri)
			.option("collection", collectionName)
			.mode("overwrite")
			.format("com.mongodb.spark.sql")
			.save()

		indexes.foreach(index => {
			if (index != null && !"".equals(index))
				collection.createIndex(MongoDBObject(index -> 1))
		})

		mongoClient.close()
	}
}
