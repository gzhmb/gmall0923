package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handle.Dauhandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.phoenix.spark._

object DauApp {
  def main(args: Array[String]): Unit = {
    //获取sparkconf
    val conf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc = new StreamingContext(conf, Seconds(5))
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //将数据转化为样例类并补全logdate和loghour字段
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDstream.mapPartitions(partition => {
      partition.map(record => {
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        val ts: Long = startUpLog.ts
        val dateHourStr: String = sdf.format(new Date(ts))
        startUpLog.logDate = dateHourStr.split(" ")(0)
        startUpLog.logHour = dateHourStr.split(" ")(1)

        startUpLog
      })
    })

    //跨批次去重
    val filterByRedisDStream: DStream[StartUpLog] = Dauhandler.filterByRedis(startUpLogDStream,ssc.sparkContext)

    /*startUpLogDStream.cache()
    filterByRedisDStream.cache()
    startUpLogDStream.count().print()
    filterByRedisDStream.count().print()*/
    //批次内去重
    val filterByMidDStream: DStream[StartUpLog] = Dauhandler.filterByMid(filterByRedisDStream)

    //将去重后的mid写入redis
    Dauhandler.saveMidToRedis(filterByMidDStream)

    //将明细数据写入hbase
   filterByMidDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix("GMALL0923_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS")
        ,HBaseConfiguration.create(),Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
