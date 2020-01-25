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

object Req2CategoryTop10SessionApplication {
  def main(args: Array[String]): Unit = {

    //准备Spark环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1CategoryTop10Application")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
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
    //println(actionRdd.count())
    // TODO  - 声明累加器，并使用累加器聚合数据
    val acc = new CategoryCountAccumulator
    //注册累加器
    spark.sparkContext.register(acc)

    actionRdd.foreach(action => {
      if (action.click_category_id != -1) {
        acc.add(action.click_category_id + "_click") //1_click
      }
      else {
        if (action.order_category_ids != null) {
          val ids = action.order_category_ids.split(",")
          for (id <- ids) {
            acc.add(id + "_order")
          }
        }
        else {
          if (action.pay_category_ids != null) {
            val ids = action.pay_category_ids.split(",")
            for (id <- ids) {
              acc.add(id + "_pay")
            }
          }
        }
      }
    })
    // 获取累加器的结果
    // (1_click, 10), (1_order, 20)
    val categorySumMap: mutable.HashMap[String, Long] = acc.value
    val categoryToGroup: Map[String, mutable.HashMap[String, Long]] = categorySumMap.groupBy {
      case (categoryAction, sum) => {
        categoryAction.split("_")(0)
      }
    }
    // println(categorySumMap.size)
    // TODO  - 将累加器的数据转化为单一的数据对象
    val taskId = UUID.randomUUID().toString

    val categoryTop10s: immutable.Iterable[CategoryTop10] = categoryToGroup.map {
      case (categoryId, groupMap) => {
        CategoryTop10(taskId, categoryId, groupMap.getOrElse(categoryId + "_click", 0), groupMap.getOrElse(categoryId + "_order", 0), groupMap.getOrElse(categoryId + "_pay", 0))
      }
    }
    categoryTop10s
    // TODO  - 对转换后的数据进行排序（点击、下单、支付）
    val sortList: List[CategoryTop10] = categoryTop10s.toList.sortWith {
      case (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        }
        else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          }
          else if (left.orderCount == right.orderCount)
            left.payCount > left.payCount
          else {
            false
          }
        }
        else {
          false
        }

      }
    }

    // TODO  - 获取前十排名数据
    val top10s: List[CategoryTop10] = sortList.take(10)
    val ids: List[String] = top10s.map(data => data.categoryId)
    //top10s.foreach(println)

    // *************************** 需求二 start *********************************************


    // TODO 4.2 将用户访问数据进行筛选过滤（ 点击，前10的品类 ）
    val filterRDD = actionRdd.filter(action => {
      if (action.click_category_id != -1) {
        ids.contains(action.click_category_id.toString)
      }
      else {
        false
      }
    })

    // TODO 4.3 将筛选过滤的数据进行结构的转换（categoryid, sessionid, click）( categoryid-sessionid,1 )

    val categoryAndSessionToClickRDD = filterRDD.map(action => {
      (action.click_category_id + "_" + action.session_id, 1)
    })

    // TODO 4.4 将转换结构后的数据进行聚合( categoryid-sessionid,1 ) ( categoryid-sessionid,sum)
    val categroyAndSessionToSumClickRDD = categoryAndSessionToClickRDD.reduceByKey(_+_)
    // TODO 4.5 将聚合后的数据进行结构转换( categoryid-sessionid,sum) ( categoryid, (sessionid,sum))
    val translationRDD = categroyAndSessionToSumClickRDD.map {
      case (key, value) => {
        val temp = key.split("_")
        (temp(0), (temp(1), value))
      }
    }
    translationRDD
    // TODO 4.6 将转换结构后的数据按照key进行分组（ categoryid，Iterator[  (sessionid,sum) ]  ）
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = translationRDD.groupByKey()

    // TODO 4.7 将分组后的数据进行排序（降序）
    // TODO 4.8 将排序后的数据获取前10条
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(datas => {
      datas.toList.sortWith {
        case (left, right) => {
          left._2 > right._2
        }
      }.take(10)
    })
    val mysqlRDD: RDD[List[CategoryTop10Session10]] = resultRDD.map {
      case (id, list) => {
        list.map {
          case (sessionId, sum) =>
            CategoryTop10Session10(taskId, id, sessionId, sum)
        }
      }
    }
    val flatRDD = mysqlRDD.flatMap(list => list)




    // *************************** 需求二 end *********************************************
    // TODO 4.6 将统计结果保存到Mysql中

    flatRDD.foreachPartition(datas =>{

      val driverClass = ConfigurationUtil.getValueFromConfig("jdbc.driver.class")
      val url = ConfigurationUtil.getValueFromConfig("jdbc.url")
      val user = ConfigurationUtil.getValueFromConfig("jdbc.user")
      val password = ConfigurationUtil.getValueFromConfig("jdbc.password")

      Class.forName(driverClass)
      val connection: Connection = DriverManager.getConnection(url, user, password)
      val statement: PreparedStatement = connection.prepareStatement("insert into category_top10_session_count values (?,?,?,?)")
      datas.foreach(data => {
        statement.setString(1,data.taskId)
        statement.setString(2, data.categoryId)
        statement.setString(3, data.sessionId)
        statement.setLong(4, data.clickCount)
        statement.executeUpdate()
      })
      statement.close()
      connection.close()

    })


    //释放资源
    spark.stop()
  }

}


case class CategoryTop10Session10( taskId:String, categoryId:String, sessionId:String, clickCount:Int ) {

}
