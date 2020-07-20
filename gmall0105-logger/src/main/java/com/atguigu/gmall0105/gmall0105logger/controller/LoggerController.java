package com.atguigu.gmall0105.gmall0105logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
@Slf4j
public class LoggerController {
    @Autowired
    KafkaTemplate kafkaTemplate;
    @RequestMapping("/applog")
    public String applog(@RequestBody String logString){
        System.out.println(logString);

        log.info(logString);
        JSONObject jsonObject = JSON.parseObject(logString);
        if (jsonObject.getString("start")!=null&&jsonObject.getString("start").length()>0){
            kafkaTemplate.send("GMALL_START_0105",logString);
        }else{
            kafkaTemplate.send("GMALL_EVENT_0105",logString);
        }


        return logString;
    }
}
