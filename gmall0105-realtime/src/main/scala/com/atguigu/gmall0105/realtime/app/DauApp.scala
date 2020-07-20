package com.atguigu.gmall0105.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0105.realtime.bean.DauInfo
import com.atguigu.gmall0105.realtime.util.{MyEsUtil, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "GMALL_START_0105"
    val groupId = "DAU_GROUP"

    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
    var recordInputStream: InputDStream[ConsumerRecord[String, String]]=null
    if(kafkaOffsetMap!=null&&kafkaOffsetMap.size>0){

       recordInputStream = MyKafkaUtil.getKafkaStream(topic, ssc,kafkaOffsetMap,groupId)
    }else{
        recordInputStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    //recordInputStream.map(_.value()).print()

    val jsonObjDstream: DStream[JSONObject] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      val ts: lang.Long = jsonObj.getLong("ts")
      val datehourString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
      val dateHour: Array[String] = datehourString.split(" ")

      jsonObj.put("dt", dateHour(0))
      jsonObj.put("hr", dateHour(1))
      jsonObj
    }
    //jsonObjDstream.print()

   /* val filteredDstream: DStream[JSONObject] = jsonObjDstream.filter { jsonObj =>
      val dt: String = jsonObj.getString("dt")
      val mid: String = jsonObj.getJSONObject("common").getString("mid")
      val jedis: Jedis = RedisUtil.getJedisClient
      val dauKey = "dau" + dt
      val isNew: lang.Long = jedis.sadd(dauKey, mid)
      jedis.close()
      if (isNew == 1L) {
        true
      } else {
        false
      }
    }*/
    //println("过滤前"+jsonObjDstream.count())
    val filteredDstream: DStream[JSONObject] = jsonObjDstream.mapPartitions { jsonObjItr =>
    val jedis: Jedis = RedisUtil.getJedisClient
      val filteredList = new ListBuffer[JSONObject]()
      val jsonList: List[JSONObject] = jsonObjItr.toList
      println("过滤前"+jsonList.size)
      for (jsonObj <- jsonList) {

        val dt: String = jsonObj.getString("dt")
        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        val dauKey = "dau" + dt
        val isNew: lang.Long = jedis.sadd(dauKey, mid)
        jedis.expire(dauKey,3600*24)
        if (isNew==1L){
          filteredList+=jsonObj
        }
      }
      jedis.close()
      println("过滤后"+filteredList.size)
      filteredList.toIterator
    }
    filteredDstream.foreachRDD { rdd =>
      rdd.foreachPartition{jsonItr=>
        val list: List[JSONObject] = jsonItr.toList
        val dauList: List[(String,DauInfo)] = list.map { jsonObj =>
          val commonJSONObj: JSONObject = jsonObj.getJSONObject("common")
          val dauInfo: DauInfo = DauInfo(commonJSONObj.getString("mid"),
            commonJSONObj.getString("uid"),
            commonJSONObj.getString("mid"),
            commonJSONObj.getString("ch"),
            commonJSONObj.getString("vc"),
            jsonObj.getString("dt"),
            jsonObj.getString("hr"),
            "00",
            jsonObj.getLong("ts")
          )

          (dauInfo.mid,dauInfo)
        }

        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
          MyEsUtil.bulkDoc(dauList,"gmall0105_dau_info_"+dt)


      }
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
