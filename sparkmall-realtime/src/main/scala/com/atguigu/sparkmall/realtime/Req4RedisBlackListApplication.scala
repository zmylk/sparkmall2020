package com.atguigu.sparkmall.realtime

import java.util

import com.atguigu.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * 广告黑名单实时统计
  */
object Req4RedisBlackListApplication {

    def main(args: Array[String]): Unit = {

        // 构建流式数据处理环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req4BlackListApplication")

        val streamingContext = new StreamingContext(sparkConf, Seconds(5))

        // 消费kafka的数据
        val topic = "ads_log"
        val kafkaMessage: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

        val messageDStream: DStream[KafkaMessage] = kafkaMessage.map {
            case record => {
                val value = record.value()
                val values = value.split(" ")
                KafkaMessage(values(0), values(1), values(2), values(3), values(4))
            }
        }
        messageDStream.print()

        // TODO 4.1 获取kafka的消费数据
        val blacklist = "blacklist"
        // TODO 4.X 将消费数据进行黑名单校验，如果用户不在黑名单中，继续访问，如果在黑名单中，直接过滤掉
        // TODO Driver (1)
        val transformDStream = messageDStream.transform(rdd => {
            // TODO Driver(N)
            // TODO Driver(N)
            val jedisClient = RedisUtil.getJedisClient
            val setDStream: util.Set[String] = jedisClient.smembers(blacklist)
            jedisClient.close()

            val broadcastDStream = streamingContext.sparkContext.broadcast(setDStream)

            rdd.filter(message => {
                // TODO Executor(M)
                !broadcastDStream.value.contains(message.userid)
            })
        })
        // TODO 4.2 向redis中进行数据的更新
        transformDStream.foreachRDD(rdd =>{
            rdd.foreachPartition( datas =>{
                val jedis = RedisUtil.getJedisClient
                datas.foreach({message: KafkaMessage =>{
                    val key = "date:adv:user:click"
                    val dateToString = DateUtil.parseTimestampToString(message.timestamp.toLong,"yyyy-MM-dd")
                    val field = dateToString+":"+":"+message.adid+":"+message.userid
                    jedis.hincrBy(key,field,1)

                    // TODO 4.3 获取当前redis中的数据统计结果
                    val sumClick = jedis.hget(key,field).toLong
                    // TODO 4.4 判断统计结果是否超过阈值
                    if (sumClick >= 100){
                        // TODO 4.5 如果超过阈值，需要将用户拉入黑名单，禁止用户下一回访问
                        jedis.sadd(blacklist,message.userid)
                    }
                }
                })
                jedis.close()
            }
            )
        })




        streamingContext.start()
        streamingContext.awaitTermination()

    }
}
case class KafkaMessage(timestamp:String, province:String, city:String, userid:String, adid:String)