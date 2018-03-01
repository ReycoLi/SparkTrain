package com.zhelisde.spark.project.util

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 156.187.29.132  2017-11-20 00:39:26   "GET www/2 HTTP/1.0"- 200
  * =》 2017-11-20 00:39:26 =》 20171120
  */
object DateUtils {
    val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    val TARGE_FORMAT = FastDateFormat.getInstance("yyyyMMdd");

    //把当前时间转换为时间戳
    def getTime(time: String)={
        YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
    }

    // 2017-11-20 00:39:26 =》 20171120
    def parseToMin(time:String) = {
        TARGE_FORMAT.format(new Date(getTime(time)))
    }

    def main(args: Array[String]): Unit = {
        print(parseToMin("2017-11-22 01:20:20"))
    }
}
