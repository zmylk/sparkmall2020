package com.atguigu.sparkmall.common.util

import java.text.SimpleDateFormat
import java.util.Date

/**
  * 日期工具类
  */
object DateUtil {

    def main(args: Array[String]): Unit = {
        parseDateToString( new Date() )
    }

    def parseDateToString( d: Date, f : String = "yyyy-MM-dd HH:mm:ss" ): String = {
        val format = new SimpleDateFormat(f)
        format.format(d)
    }

    def parseTimestampToString( ts: Long, f : String = "yyyy-MM-dd HH:mm:ss" ): String = {
        parseDateToString(new Date(ts), f)
    }
}
