package com.atguigu.handle

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object Dauhandler {
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {

    startUpLogDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        //获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        partition.foreach(log=>{
          //写库
          val redisKey = "DAU:"+log.logDate
          jedisClient.sadd(redisKey,log.mid)
        })
        //归还连接
        jedisClient.close()
      })
    })
  }

  //批次内去重
    def filterByMid(filterByRedisDStream: DStream[StartUpLog]) = {
    //1.转换数据结构（（mid，logdate），StartUpLog）
    val midToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(log => {
      ((log.mid, log.logDate), log)
    })
    //2.使用groupByKey将相同key聚合到同一分区
    val midToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midToLogDStream.groupByKey()

    //3.排序取出第一条数据，达到去重的效果
    val midToLogList: DStream[((String, String), List[StartUpLog])] = midToLogIterDStream.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })

    //4.扁平化
    val result: DStream[StartUpLog] = midToLogList.flatMap(_._2)
    result
  }
  //跨批次去重
  def filterByRedis(startUpLogDStream:DStream[StartUpLog],sc : SparkContext)={
    /*val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
      //获取redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //过滤数据
      val redisKey = "DAU:" + log.logDate
      val boolean: lang.Boolean = jedisClient.sismember(redisKey, log.mid)
      //释放连接
      jedisClient.close()
      !boolean
    })
    value*/
    // TODO 方案三
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    startUpLogDStream.transform(rdd=>{
      //获取连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //获取数据
      val redisKey = "DAU:"+ sdf.format(new Date(System.currentTimeMillis()))
      val midSet: util.Set[String] = jedisClient.smembers(redisKey)

      //归还连接
      jedisClient.close()
      //广播数据
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(midSet)
      //过滤数据
      val result: RDD[StartUpLog] = rdd.filter(log => {
        !midBC.value.contains(log.mid)
      })
      result
    })
  }

}
