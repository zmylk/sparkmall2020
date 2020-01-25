package com.atguigu.sparkmall.realtime

import com.atguigu.sparkmall.common.util.RedisUtil

object LikeTest {
  def main(args: Array[String]): Unit = {
    val jedisClient = RedisUtil.getJedisClient
    jedisClient.sadd("like","like")
    jedisClient.close()
  }
}
