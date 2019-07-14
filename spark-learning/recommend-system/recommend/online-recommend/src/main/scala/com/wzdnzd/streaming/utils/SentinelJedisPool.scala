/**
  * @Author : wzdnzd
  * @Time :  2019-07-13
  * @Project : bigdata
  */

package com.wzdnzd.streaming.utils

import java.util

import com.wzdnzd.config.ConstantValue
import redis.clients.jedis.{Jedis, JedisPoolConfig, JedisSentinelPool}


object SentinelJedisPool {
	lazy val pool: JedisSentinelPool = createJedisPool()

	def createJedisPool(): JedisSentinelPool = this.synchronized {
		val config: JedisPoolConfig = new JedisPoolConfig()
		config.setMaxTotal(10)
		config.setMaxIdle(3)
		config.setMaxWaitMillis(3000)

		val sentinels: util.HashSet[String] = new util.HashSet()
		val redisServers = ConstantValue.CONFIG("redis.server").split(",")
		redisServers.filter(server => server != null && !"".equals(server)).foreach(server => sentinels.add(server.trim))
		new JedisSentinelPool(ConstantValue.CONFIG("redis.cluster"), sentinels, config, 5000, ConstantValue.CONFIG("redis.password"))
	}

	def getJedis(): Jedis = {
		if (pool == null || pool.isClosed)
			createJedisPool()
		pool.getResource
	}
}
