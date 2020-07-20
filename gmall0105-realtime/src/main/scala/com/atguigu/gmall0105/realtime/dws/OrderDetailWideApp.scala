package com.atguigu.gmall0105.realtime.dws



import java.util.Properties
import java.{lang, util}

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall0105.realtime.bean.{OrderDetail, OrderDetailWide, OrderInfo}
import com.atguigu.gmall0105.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object OrderDetailWideApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("order_wide_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topicOrderInfo = "DWD_ORDER_INFO"
    val topicOrderDetail = "DWD_ORDER_DETAIL"
    val groupIdorderInfo = "dws-order_info_group"
    val groupIdorderDetail = "dws-order_detail_group"

    val orderInfokafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topicOrderInfo, groupIdorderInfo)
    var orderInfoRecordInputStream: InputDStream[ConsumerRecord[String, String]]=null
    if(orderInfokafkaOffsetMap!=null&&orderInfokafkaOffsetMap.size>0){

      orderInfoRecordInputStream = MyKafkaUtil.getKafkaStream(topicOrderInfo, ssc,orderInfokafkaOffsetMap,groupIdorderInfo)
    }else{
      orderInfoRecordInputStream = MyKafkaUtil.getKafkaStream(topicOrderInfo,ssc,groupIdorderInfo)
    }

    val orderDetailkafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topicOrderDetail, groupIdorderDetail)
    var orderDetailRecordInputStream: InputDStream[ConsumerRecord[String, String]]=null
    if(orderDetailkafkaOffsetMap!=null&&orderDetailkafkaOffsetMap.size>0){

      orderDetailRecordInputStream = MyKafkaUtil.getKafkaStream(topicOrderDetail, ssc,orderDetailkafkaOffsetMap,groupIdorderDetail)
    }else{
      orderDetailRecordInputStream = MyKafkaUtil.getKafkaStream(topicOrderDetail,ssc,groupIdorderDetail)
    }

    var orderInfoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoRecordInputStream.transform { rdd =>
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    var orderDetailOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderDetailInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailRecordInputStream.transform { rdd =>
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val orderInfoDstream: DStream[OrderInfo] = orderInfoInputGetOffsetDstream.map { record =>
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

      orderInfo
    }

    val orderDetailDstream: DStream[OrderDetail] = orderDetailInputGetOffsetDstream.map { record =>
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])

      orderDetail
    }

    orderInfoDstream.print(1000)
    orderDetailDstream.print(1000)
    val orderInfoWithKeyDStream: DStream[(Long, OrderInfo)] = orderInfoDstream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailWithKeyDStream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail => (orderDetail.order_id, orderDetail))
    //val orderJoinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDStream.join(orderDetailWithKeyDStream)
    val orderInfoWithKeyWindowDstream: DStream[(Long, OrderInfo)] = orderInfoWithKeyDStream.window(Seconds(10), Seconds(5))
    val orderDetailWithKeyWindowDstream: DStream[(Long, OrderDetail)] = orderDetailWithKeyDStream.window(Seconds(10), Seconds(5))

    val orderJoinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyWindowDstream.join(orderDetailWithKeyWindowDstream)
    val orderJoinedNewDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderJoinedDstream.mapPartitions { orderJoinedTupleItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val key = "order_join_keys"
      val orderJoinedNewList = new ListBuffer[(Long, (OrderInfo, OrderDetail))]
      for ((orderId, (orderInfo, orderDetail)) <- orderJoinedTupleItr) {
        val ifNew: lang.Long = jedis.sadd(key, orderDetail.id.toString)
        if (ifNew == 1L) {
          orderJoinedNewList.append((orderId, (orderInfo, orderDetail)))
        }

      }
      jedis.close()
      orderJoinedNewList.toIterator

    }

    val orderDetailWideDStream: DStream[OrderDetailWide] = orderJoinedNewDstream.map{case(orderId,(orderInfo,orderDetail))=>new OrderDetailWide(orderInfo,orderDetail)}
    //orderDetailWideDStream.print(1000)
    val orderWideWithSplitDstream: DStream[OrderDetailWide] = orderDetailWideDStream.mapPartitions { orderWideItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      //    1  先从redis取 两个合计    【实付分摊金额】的合计，【数量*单价】的合计
      val orderWideList: List[OrderDetailWide] = orderWideItr.toList
      for (orderWide <- orderWideList) {
        // type ?   hash      key? order_split_amount:[order_id]  field split_amount_sum ,origin_amount_sum    value  ?  累积金额
        val key = "order_split_amount:" + orderWide.order_id
        val orderSumMap: util.Map[String, String] = jedis.hgetAll(key)
        var splitAmountSum = 0D
        var originAmountSum = 0D
        if (orderSumMap != null && orderSumMap.size() > 0) {
          val splitAmountSumString: String = orderSumMap.get("split_amount_sum")
          splitAmountSum = splitAmountSumString.toDouble

          val originAmountSumString: String = orderSumMap.get("origin_amount_sum")
          originAmountSum = originAmountSumString.toDouble
        }
        //    2 先判断是否是最后一笔  ： （数量*单价）== 原始总金额 -（其他明细 【数量*单价】的合计）
        val detailOrginAmount: Double = orderWide.sku_num * orderWide.sku_price //单条明细的原始金额  数量*单价
        val restOriginAmount: Double = orderWide.final_total_amount - originAmountSum
        if (detailOrginAmount == restOriginAmount) {
          //3.1  最后一笔 用减法 ：实付分摊金额= 实付总金额 - （其他明细已经计算好的【实付分摊金额】的合计）
          orderWide.final_detail_amount = orderWide.final_total_amount - splitAmountSum
        } else {
          //3.2  不是最后一笔 用乘除  实付分摊金额=（数量*单价）*实付总金额 / 原始总金额
          orderWide.final_detail_amount = detailOrginAmount * orderWide.final_total_amount / orderWide.original_total_amount
          orderWide.final_detail_amount= Math.round(orderWide.final_detail_amount*100D)/100D
        }
        //    4  进行合计保存
        splitAmountSum += orderWide.final_detail_amount
        originAmountSum += detailOrginAmount
        orderSumMap.put("split_amount_sum", splitAmountSum.toString)
        orderSumMap.put("origin_amount_sum", originAmountSum.toString)
        jedis.hmset(key, orderSumMap)
      }
      jedis.close()
      orderWideList.toIterator
    }
    orderWideWithSplitDstream.cache()
    orderWideWithSplitDstream.map(orderwide=>JSON.toJSONString(orderwide,new SerializeConfig(true))).print(1000)


    val sparkSession = SparkSession.builder()
      .appName("order_detail_wide_spark_app")
      .getOrCreate()

    import sparkSession.implicits._

    val orderWideKafkaSentDstream: DStream[OrderDetailWide] = orderWideWithSplitDstream.mapPartitions { orderWideItr =>
      val orderWideList: List[OrderDetailWide] = orderWideItr.toList
      for (orderWide <- orderWideList) {
        MyKafkaSink.send("DWS_ORDER_WIDE", JSON.toJSONString(orderWide, new SerializeConfig(true)))
      }
      orderWideList.toIterator
    }


    orderWideKafkaSentDstream.foreachRDD{rdd=>
      val df: DataFrame = rdd.toDF()
      df.write.mode(SaveMode.Append)
        .option("batchsize", "100")
        .option("isolationLevel", "NONE") // 设置事务
        .option("numPartitions", "4") // 设置并发
        .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
        .jdbc("jdbc:clickhouse://hadoop102:8123/test0105","order_wide_0105",new Properties())
      OffsetManager.saveOffset(topicOrderInfo,groupIdorderInfo,orderInfoOffsetRanges)
      OffsetManager.saveOffset(topicOrderDetail,groupIdorderDetail,orderDetailOffsetRanges)
    }



    ssc.start()
    ssc.awaitTermination()
  }
}
