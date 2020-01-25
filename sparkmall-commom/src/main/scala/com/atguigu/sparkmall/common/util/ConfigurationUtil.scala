package com.atguigu.sparkmall.common.util

import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * 配置工具类
  */
object ConfigurationUtil {

    def main(args: Array[String]): Unit = {
        println(getValueFromConfig("hive.database","config"))
        println(getValueFromCondition("startDate"))
    }

    def getValueFromCondition(key : String ): String = {
        val bundle: ResourceBundle = ResourceBundle.getBundle("condition")
        val condition: String = bundle.getString("condition.params.json")
        val obj: JSONObject = JSON.parseObject(condition)
        obj.getString(key)
    }

    def getValueFromConfig(key : String, path:String = "config" ): String = {
        /*
        Java中最基本的读取配置文件方式
        val stream: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(path)
        val properties = new Properties()
        properties.load(stream)

        properties.getProperty(key)
        */

        // 读取配置文件：国际化（i18n）
        val bundle: ResourceBundle = ResourceBundle.getBundle(path)
        bundle.getString(key)
    }
}
