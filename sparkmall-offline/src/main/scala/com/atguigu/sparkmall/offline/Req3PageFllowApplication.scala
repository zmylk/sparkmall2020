package com.atguigu.sparkmall.offline

import java.util.UUID
import java.sql.{Connection, DriverManager, PreparedStatement}
import com.atguigu.sparkmall.common.model.UserVisitAction
import com.atguigu.sparkmall.common.util.ConfigurationUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

object Req3PageFllowApplication {
  def main(args: Array[String]): Unit = {

    //准备Spark环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1CategoryTop10Application")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    spark.sparkContext.setCheckpointDir("cp")
    import spark.implicits._

    // TODO  - 从hive中获取用户点击信息

    val startDate = ConfigurationUtil.getValueFromCondition("startDate")
    val endDate = ConfigurationUtil.getValueFromCondition("endDate")


    var sql = "select * from user_visit_action where 1=1 "
    if (startDate != null) {
      sql = sql + "and action_time >= '" + startDate + "'"
    }
    if (endDate != null) {
      sql = sql + "and action_time <= '" + endDate + "'"
    }
    spark.sql("use " + ConfigurationUtil.getValueFromConfig("hive.database"))
    val dataFrame: DataFrame = spark.sql(sql)
    val dataSet: Dataset[UserVisitAction] = dataFrame.as[UserVisitAction]
    val actionRdd: RDD[UserVisitAction] = dataSet.rdd
    actionRdd.checkpoint()
    //println(actionRdd.count())
    // TODO 计算分母数据
    // TODO 4.1 获取用户访问数据，进行过滤，保留需要进行统计的数据
    val pageid = ConfigurationUtil.getValueFromCondition("targetPageFlow")
    val pageids = pageid.split(",")

    val zipPageids = pageids.zip(pageids.tail).map {
      case (id1, id2) => {}
        id1 + "-" + id2
    }


    val filteractionrdd = actionRdd.filter { action => {
      pageids.contains(action.page_id.toString)
    }
    }

    // TODO 4.2 将每一个页面的点击进行聚合，获取结果的分母数据
    val mapRDD = filteractionrdd.map {
      case action => {
        (action.page_id, 1)
      }
    }
    val pageIdToSumRDD = mapRDD.reduceByKey(_+_)
    val pidToSumMap = pageIdToSumRDD.collect().toMap

    // TODO 计算分子数据
    // 将数据保存到检查点中
    // TODO 4.3 获取用户访问数据，对sessionid进行分组
    val groupRDD = actionRdd.groupBy( action => action.session_id)

    // TODO 4.4 对分组后的数据进行时间排序（升序）
    val zipRDD = groupRDD.mapValues { data => {
      val actions = data.toList.sortWith {
        case (left, right) => {
          left.action_time < right.action_time
        }
      }
      val ids = actions.map(_.page_id)
      val idsToids = ids.zip(ids.tail)
      idsToids
    }
    }

    // TODO 4.5 将排序后的页面数据进行拉链处理（12,23,34）
    val zipMapRDD = zipRDD.map(_._2)
    val flatMapRDD = zipMapRDD.flatMap(list => list)
    // 过滤，保留需要关心跳转的页面（1-2,2-3,3-4,4-5,5-6,6-7）
    val zipfilterRDD = flatMapRDD.filter {
      case (pid1, pid2) => {
        zipPageids.contains(pid1 + "-" + pid2)
      }
    }

    // TODO 4.6 将拉链后的数据进行结构转换（  （12, 1）, （23, 1） ）

    val zipmapmapRDD = zipfilterRDD.map {
      case (pid1, pid2) => {
        (pid1 + "-" + pid2, 1)
      }
    }

    // TODO 4.7 将转换结构后的数据进行聚合（  （12, 10）, （23, 100） ），获取分子数据
    val reduceByKeyRDD = zipmapmapRDD.reduceByKey(_+_)
    // TODO 4.8 将分子数据除以分母数据，获取最终的结果
    reduceByKeyRDD.foreach{
      case (pids,sum) =>{
        val pidA = pids.split("-")(0)
        println(pids +"="+ sum.toDouble/pidToSumMap(pidA.toLong))
      }
    }




    //释放资源
    spark.stop()
  }

}

