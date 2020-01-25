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
object Req4BlackListApplication {

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
        // 将过滤后的数据进行结构的转换，为了方便统计
        val dateAndadidAnduseridToClick: DStream[(String, Int)] = transformDStream.map(message => {
            val dateString = DateUtil.parseTimestampToString(message.timestamp.toLong)
            (dateString + "-" + message.adid + "-" + message.userid, 1)
        })



        //使用有状态操作
        streamingContext.sparkContext.setCheckpointDir("cp")

        val dateAndAdvAndUserToSumDStream: DStream[(String, Int)] = dateAndadidAnduseridToClick.updateStateByKey {
            case (seq, buffer) => {
                val total = seq.sum + buffer.getOrElse(0)
                Option(total)
            }
        }
        dateAndAdvAndUserToSumDStream.foreachRDD(rdd =>{
            rdd.foreach{
                case (key, sum) =>{
                    if (sum >=5)
                        {
                            val contectRedis = RedisUtil.getJedisClient
                            val strings = key.split("-")
                            contectRedis.sadd(blacklist,strings(2))
                            contectRedis.close()
                        }
                }
            }
        })

        streamingContext.start()
        streamingContext.awaitTermination()

    }
}