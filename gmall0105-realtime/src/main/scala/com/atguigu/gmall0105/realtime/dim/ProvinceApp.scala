package com.atguigu.gmall0105.realtime.dim

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0105.realtime.bean.dim.ProvinceInfo
import com.atguigu.gmall0105.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.phoenix.spark._
object ProvinceApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("province_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_BASE_PROVINCE"
    val groupId = "province_app_group"

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

    inputGetOffsetDstream.foreachRDD{rdd=>
      val provinceInfoRDD: RDD[ProvinceInfo] = rdd.map { record =>
        val jsonString: String = record.value()
        val provinceInfo: ProvinceInfo = JSON.parseObject(jsonString, classOf[ProvinceInfo])
        provinceInfo
      }
      provinceInfoRDD.saveToPhoenix("gmall0105_province_info",
        Seq("ID","NAME","AREA_CODE","ISO_CODE","ISO_3166_2"),new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
