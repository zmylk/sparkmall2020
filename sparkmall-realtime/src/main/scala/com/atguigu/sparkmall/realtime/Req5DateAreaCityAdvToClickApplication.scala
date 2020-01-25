package com.atguigu.sparkmall.realtime



import com.atguigu.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * 每天各地区各城市各广告的点击流量实时统计
  */
object Req5DateAreaCityAdvToClickApplication {

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

    //    val mapDStream: DStream[(String, Int)] = messageDStream.map(message => {
    //      val dateToString = DateUtil.parseTimestampToString(message.timestamp.toLong, "yyyy-MM-dd")
    //      val filed = dateToString + ":" + message.province + ":" + message.city + ":" + message.adid
    //      (filed,1)
    //    })
    val mapDStream: DStream[(String, Int)] = messageDStream.map {
      data => {
        val dateString: String = DateUtil.parseTimestampToString(data.timestamp.toLong, "yyyy-MM-dd")
        val field = dateString + ":" + data.province + ":" + data.city + ":" + data.adid

        (field, 1)
      }
    }
    streamingContext.sparkContext.setCheckpointDir("cp")

    //        val updateStateByKeyDStream = mapDStream.updateStateByKey {
    //            case (seq, buffer) => {
    //                val total = seq.sum + buffer.getOrElse(0)
    //                Option(total)
    //            }
    //        }
    val stateDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
      case (seq, buffer) => {
        val total = seq.sum + buffer.getOrElse(0)
        Option(total)
      }
    }
    stateDStream.foreachRDD(rdd => {
      rdd.foreachPartition(message => {
        val jedisClient = RedisUtil.getJedisClient
        val key = "date:area:city:ads"
        message.foreach(data => {
          jedisClient.hset(key, data._1, data._2.toString)
        })
        jedisClient.close()
      })
    })
    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
