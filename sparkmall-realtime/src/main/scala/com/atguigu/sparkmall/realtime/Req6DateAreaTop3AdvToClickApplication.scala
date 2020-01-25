package com.atguigu.sparkmall.realtime

import com.atguigu.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
  * 每天各地区 top3 热门广告
  */
object Req6DateAreaTop3AdvToClickApplication {

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

        val mapDStream: DStream[(String, Int)] = messageDStream.map {
            data => {
                val dateString: String = DateUtil.parseTimestampToString(data.timestamp.toLong, "yyyy-MM-dd")
                val field = dateString + ":" + data.province + ":" + data.city + ":" + data.adid

                (field, 1)
            }
        }
        streamingContext.sparkContext.setCheckpointDir("cp")
        val stateDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
            case (seq, buffer) => {
                val total = seq.sum + buffer.getOrElse(0)
                Option(total)
            }
        }
        // ************************* 需求六 start **********************************************
        // TODO 4.1 获取需求五的数据

        // TODO 4.2 将数据进行结构的转换（date-area-city-adv, sum）(date-area-adv, sum)
        val mapDStreamNocity= stateDStream.map({
            case (key, value) => {
                val keys = key.split(":")
                (keys(0) + "_" + keys(1) + "_" + keys(3), value)
            }
        })
        // TODO 4.3 将转换结构后的数据进行统计（ date-area-adv, totalsum ）
        val reduceByKeyDStream = mapDStreamNocity.reduceByKey(_+_)

        // TODO 4.4 将统计后的结果转换结构（date-area-adv, totalsum） (date-area, (adv,totalsum) )
        val mapDStreamWaitGP = reduceByKeyDStream.map {
            case (key, values) => {
                val keys = key.split("_")
                (keys(0) + "_" + keys(1), (keys(2), values))
            }
        }


        // TODO 4.5 将转换结构后的数据进行分组（ date-area, Iterator[ ( adv, totalsum ) ] ）
        val groupByKeyDStrea: DStream[(String, Iterable[(String, Int)])] = mapDStreamWaitGP.groupByKey()
        // TODO 4.6 将分组后的数据进行排序（ 降序 ）
        val mapValuesDStream = groupByKeyDStrea.mapValues { datas => {
            datas.toList.sortWith {
                case (left, right) => {
                    left._2 > right._2
                }
            }.take(3).toMap
        }
        }
        mapValuesDStream
        // TODO 4.8 将结果保存到redis中
        mapValuesDStream.foreachRDD(rdd =>{
            rdd.foreachPartition(datas =>{
                val jedisClient = RedisUtil.getJedisClient

                for ((key,value) <- datas){

                    println(key+":::::"+value)
                    val ks = key.split("_")
                    val k:String = "top3_ads_per_day:" + ks(0)
                    val field = ks(1)
                    import org.json4s.JsonDSL._
                    val listString: String = JsonMethods.compact(JsonMethods.render(value))
                    jedisClient.hset(k,field,listString)
                }
                jedisClient.close()
            })
        })


        // ************************* 需求六 end **********************************************

        streamingContext.start()
        streamingContext.awaitTermination()

    }
}
