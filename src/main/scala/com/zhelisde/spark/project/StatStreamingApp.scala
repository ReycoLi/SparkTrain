package com.zhelisde.spark.project

import com.zhelisde.spark.dao.{CategoryClickCountDAO, CategorySearchClickCountDAO}
import com.zhelisde.spark.domain.{CategoryClickCount, CategorySearchClickCount, ClickLog}
import com.zhelisde.spark.project.util.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.collection.mutable.ListBuffer



object StatStreamingApp {
    def main(args: Array[String]): Unit = {
        val ssc = new StreamingContext("local[*]", "StatStreamingApp", Seconds(5))
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "192.168.25.211:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "test",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val topics = Array("Flumetopic")
        val logs = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        ).map(_.value())

        //记录结构
        //156.187.29.132  2017-11-20 00:39:26   "GET /www/2 HTTP/1.0"- 200

        val cleanLog = logs.map(line => {
            val infos = line.split("\t")
            val url = infos(2).split(" ")(1)
            var categoryId = 0
            if (url.startsWith("www")) {
                categoryId = url.split("/")(1).toInt
            }
            ClickLog(infos(0), DateUtils.parseToMin(infos(1)), categoryId, infos(3), infos(4).toInt)
        }).filter(log => log.categoryId != 0)
        cleanLog.print()

        //需求1：每个类别每天的点击量 需要day, categoryId
        cleanLog.map(log =>{
            (log.time.substring(0,8)+"_"+log.categoryId,1)
        }).reduceByKey(_+_).foreachRDD(rdd => {
            rdd.foreachPartition(partition => {
                val list = new ListBuffer[CategoryClickCount]
                partition.foreach(pair =>{
                    list.append(CategoryClickCount(pair._1, pair._2))
                })
                CategoryClickCountDAO.save(list)
            })
        })

        //需求2： 每个栏目下面，从不同reference来的流量， 需要时间，来源
        // 20171122_1（渠道）_1(类别) 100（点击量）
        //需要新建hbase table category_search_count
        //create ""category_search_count","info"
        cleanLog.map(log => {
            val url = log.refer.replace("//", "/")
            val splits = url.split("/")
            var host = ""
            if(splits.length > 2){
                host = splits(1)
            }
            (host, log.time, log.categoryId)
        }).filter(x => x._1 != "").map(x => {
            (x._2.substring(0,8)+ "_" + x._1 + "_" + x._3, 1)
        }).reduceByKey(_+_).foreachRDD(rdd => {
            rdd.foreachPartition(partition => {
                val list = new ListBuffer[CategorySearchClickCount]
                partition.foreach(pair => {
                    list.append(CategorySearchClickCount(pair._1, pair._2))
                })
                CategorySearchClickCountDAO.save(list)
            })
        })

        ssc.start()
        ssc.awaitTermination()
    }
}

