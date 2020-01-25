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

object Req1CategoryTop10Application {
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

    //top10s.foreach(println)

    // TODO  - 将统计结果存入MYSQL中
    val driverClass = ConfigurationUtil.getValueFromConfig("jdbc.driver.class")
    val url = ConfigurationUtil.getValueFromConfig("jdbc.url")
    val user = ConfigurationUtil.getValueFromConfig("jdbc.user")
    val password = ConfigurationUtil.getValueFromConfig("jdbc.password")

    Class.forName(driverClass)
    val connection: Connection = DriverManager.getConnection(url,user,password)
    val statement: PreparedStatement = connection.prepareStatement("insert into category_top10 values (?,?,?,?,?)")
    top10s.foreach(data => {
      statement.setString(1,data.taskId)
      statement.setString(2,data.categoryId)
      statement.setLong(3,data.clickCount)
      statement.setLong(4,data.orderCount)
      statement.setLong(5,data.payCount)
      statement.executeUpdate()
    })
    statement.close()
    connection.close()
    //释放资源
    spark.stop()
  }

}

case class CategoryTop10(taskId: String, categoryId: String, clickCount: Long, orderCount: Long, payCount: Long) {

}


class CategoryCountAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {
  var map = new mutable.HashMap[String, Long]()

  override def isZero: Boolean = {
    map.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new CategoryCountAccumulator
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(v: String): Unit = {
    map(v) = map.getOrElse(v, 0L) + 1
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    var map1 = map
    var map2 = other.value

    map = map1.foldLeft(map2) {
      case (tempMap, (k, sumCount)) => {
        tempMap(k) = tempMap.getOrElse(k, 0L) + sumCount
        tempMap
      }
    }
  }

  override def value: mutable.HashMap[String, Long] = map
}
