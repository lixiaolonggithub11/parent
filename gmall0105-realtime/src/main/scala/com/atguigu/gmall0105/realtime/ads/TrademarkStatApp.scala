package com.atguigu.gmall0105.realtime.ads

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0105.realtime.bean.OrderDetailWide
import com.atguigu.gmall0105.realtime.util.{MyKafkaUtil, OffsetManager, OffsetManagerM}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

object TrademarkStatApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("trademark_stat_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "DWS_ORDER_WIDE"
    val groupId = "trademark_stat_group"

    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManagerM.getOffset(topic, groupId)
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if (kafkaOffsetMap != null && kafkaOffsetMap.size > 0) {

      recordInputStream = MyKafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId)
    } else {
      recordInputStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    val jsonObjDstream: DStream[OrderDetailWide] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val orderDetailWide: OrderDetailWide = JSON.parseObject(jsonString,classOf[OrderDetailWide])
      orderDetailWide
    }
    jsonObjDstream.print(100)

    val amountWithTmDstream: DStream[(String, Double)] = jsonObjDstream.map(orderWide => (orderWide.tm_id + ":" + orderWide.tm_name, orderWide.final_detail_amount))
    val amountByTmDstream: DStream[(String, Double)] = amountWithTmDstream.reduceByKey(_ + _)

    amountByTmDstream.foreachRDD{rdd=>
      val amountArray: Array[(String, Double)] = rdd.collect()
      if(amountArray!=null&& amountArray.size>0){
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        DBs.setup()
        DB.localTx(implicit session=>{// 此括号内的代码 为原子事务
          //sql1
          for ((tm,amount) <- amountArray ) {
            ///写数据库
            val tmArr: Array[String] = tm.split(":")
            val tmId=tmArr(0)
            val tmName=tmArr(1)
            val statTime: String = simpleDateFormat.format(new Date())
            println("数据写入 执行")
            SQL("insert into trademark_amount_stat values (?,?,?,?) ").bind(statTime,tmId,tmName,amount).update().apply()
          }

          //sql2  //提交偏移量
          for (offsetRange <- offsetRanges ) {
            val partitionId: Int = offsetRange.partition
            val untilOffset: Long = offsetRange.untilOffset
            println("偏移量提交 执行")
            SQL("REPLACE INTO  offset_0105(group_id,topic,partition_id,topic_offset)  VALUES(?,?,?,?) ")
              .bind(groupId,topic,partitionId,untilOffset).update().apply()
          }
        })
      }
    }
    ssc.start()
    ssc.awaitTermination()

  }
}
