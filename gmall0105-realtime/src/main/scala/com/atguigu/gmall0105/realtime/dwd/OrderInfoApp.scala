package com.atguigu.gmall0105.realtime.dwd

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0105.realtime.bean.OrderInfo
import com.atguigu.gmall0105.realtime.bean.dim.{ProvinceInfo, UserState}
import com.atguigu.gmall0105.realtime.util.{MyEsUtil, MyKafkaSink, MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.phoenix.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
object OrderInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("order_info_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_ORDER_INFO"
    val groupId = "order_info_group"

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
    val orderInfoDstream: DStream[OrderInfo] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date=createTimeArr(0)
      val timeArr: Array[String] = createTimeArr(1).split(":")
      orderInfo.create_hour=timeArr(0)
      orderInfo
    }
    val orderInfoWithFirstFlagDstream: DStream[OrderInfo] = orderInfoDstream.mapPartitions { orderInfoItr =>
      //每分区的操作
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      if(orderInfoList.size>0){
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        val sql = "select user_id , if_consumed from user_state0105 where user_id in ('" + userIdList.mkString("','") + "')"
        val userStateList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val userStateMap: Map[String, String] = userStateList.map(jsonObj => (jsonObj.getString("USER_ID"), jsonObj.getString("IF_CONSUMED"))).toMap
        for (orderInfo <- orderInfoList) { //每条数据
          //得到是否消费
          val if_consumed: String = userStateMap.getOrElse(orderInfo.user_id.toString, null)
          if (if_consumed != null && if_consumed == "1") { //如果是消费用户  首单标志置为0
            orderInfo.if_first_order = "0";
          } else {
            orderInfo.if_first_order = "1";
          }
        }
      }
      orderInfoList.toIterator
    }

    val OrderInfoWithKey: DStream[(Long, OrderInfo)] = orderInfoWithFirstFlagDstream.map(orderInfo => (orderInfo.user_id, orderInfo))
    val orderInfoGroupByUidDstream: DStream[(Long, Iterable[OrderInfo])] = OrderInfoWithKey.groupByKey()
    val orderInfoWithFirstRealFlagDstream: DStream[OrderInfo] = orderInfoGroupByUidDstream.flatMap { case (userId, orderInfoItr) =>
      if (orderInfoItr.size > 1) {
        val userOrderInfoSortedList: List[OrderInfo] = orderInfoItr.toList.sortWith((orderInfo1, orderInfo2) => orderInfo1.create_time < orderInfo2.create_time)
        val orderInfoFirst: OrderInfo = userOrderInfoSortedList(0)
        if (orderInfoFirst.if_first_order == "1") {
          for (i <- 1 to userOrderInfoSortedList.size - 1) {
            val orderInfoNotFirst: OrderInfo = userOrderInfoSortedList(i)
            orderInfoNotFirst.if_first_order = "0"
          }
        }
        userOrderInfoSortedList
      } else {
        orderInfoItr.toList
      }
    }
    //优化
    val orderInfoWithProvinceDstream: DStream[OrderInfo] = orderInfoWithFirstRealFlagDstream.transform { rdd =>
      val sql = "select id , name,area_code,iso_code,iso_3166_2 from gmall0105_province_info"
      val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
      val provinceMap: Map[String, ProvinceInfo] = provinceInfoList.map { jsonObj =>
        val provinceInfo: ProvinceInfo = ProvinceInfo(jsonObj.getString("ID"),
          jsonObj.getString("NAME"),
          jsonObj.getString("AREA_CODE"),
          jsonObj.getString("ISO_CODE"),
          jsonObj.getString("ISO_3166_2")

        )
        (provinceInfo.id, provinceInfo)
      }.toMap
      val provinceBC: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceMap)
      val orderInfoWithProvinceRDD: RDD[OrderInfo] = rdd.map { orderInfo =>
        val provinceMap: Map[String, ProvinceInfo] = provinceBC.value
        val provinceInfo: ProvinceInfo = provinceMap.getOrElse(orderInfo.province_id.toString, null)
        if(provinceInfo!=null) {
          orderInfo.province_name = provinceInfo.name
          orderInfo.province_area_code = provinceInfo.area_code
          orderInfo.province_iso_code = provinceInfo.iso_code
          orderInfo.province_iso_3166_2 = provinceInfo.iso_3166_2
        }
        orderInfo
      }
      orderInfoWithProvinceRDD
    }

    orderInfoWithProvinceDstream.print(1000)

    val orderInfoWithUserDstream: DStream[OrderInfo] = orderInfoWithProvinceDstream.mapPartitions { orderInfoItr =>
      val orderList: List[OrderInfo] = orderInfoItr.toList
      if(orderList.size>0) {
        val userIdList: List[Long] = orderList.map(_.user_id)
        val sql = "select id ,user_level ,  birthday  , gender  , age_group  , gender_name from gmall0105_user_info where id in ('" + userIdList.mkString("','") + "')"
        val userJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val userJsonObjMap: Map[Long, JSONObject] = userJsonObjList.map(userJsonObj => (userJsonObj.getLongValue("ID"), userJsonObj)).toMap
        for (orderInfo <- orderList) {
          val userJsonObj: JSONObject = userJsonObjMap.getOrElse(orderInfo.user_id, null)
          if(userJsonObj!=null) {
            orderInfo.user_age_group = userJsonObj.getString("AGE_GROUP")
            orderInfo.user_gender = userJsonObj.getString("GENDER_NAME")
          }
        }
      }
      orderList.toIterator
    }

/*    orderInfoWithFirstRealFlagDstream.mapPartitions{orderInfoItr=>
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      val provinceIdList: List[Long] = orderInfoList.map(_.province_id)
      val sql = "select id , name,area_code,iso_code from gmall0105_province_info where id in ('" + provinceIdList.mkString("','") + "')"
      val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
      provinceIdList
    }*/


    orderInfoWithUserDstream.cache()

    orderInfoWithUserDstream.foreachRDD{rdd=>
      val newConsumeUserRdd: RDD[UserState] = rdd.filter(_.if_first_order == "1").map(orderInfo => UserState(orderInfo.user_id.toString, "1"))
      newConsumeUserRdd.saveToPhoenix("user_state0105",Seq("USER_ID","IF_CONSUMED"),
        new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))


    }

    orderInfoWithUserDstream.foreachRDD{rdd=>
      rdd.foreachPartition{orderInfoItr=>
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        val orderInfoWithIdList: List[(String, OrderInfo)] = orderInfoList.map(orderInfo => (orderInfo.id.toString, orderInfo))
        val dateString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        MyEsUtil.bulkDoc(orderInfoWithIdList,"gmall0105_order_info_"+dateString)
        for(orderInfo<- orderInfoList){
          val orderInfoJsonString: String = JSON.toJSONString(orderInfo,new SerializeConfig(true))
          MyKafkaSink.send("DWD_ORDER_INFO",orderInfo.id.toString,orderInfoJsonString)
        }
        
      }
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
