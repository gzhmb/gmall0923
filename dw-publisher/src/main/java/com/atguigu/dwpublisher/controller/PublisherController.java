package com.atguigu.dwpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.dwpublisher.service.PublisherService;
import com.google.inject.internal.util.$AsynchronousComputationException;
import org.apache.avro.data.Json;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {
    @Autowired
    private PublisherService publisherService;
    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date")String date){
        //创建list集合用来保存结果数据
        ArrayList<Map> result = new ArrayList<>();
        //创建map存放新增日活返回数据
        HashMap<String, Object> dauMap = new HashMap<>();
        //创建map存放新增设备返回数据
        HashMap<String, Object> devMap = new HashMap<>();
        //创建map存放新增gmv返回数据
        HashMap<String, Object> gmvMap = new HashMap<>();

        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",publisherService.getDauTotal(date));

        devMap.put("id","new_mid");
        devMap.put("name","新增设备");
        devMap.put("value",233);

        gmvMap.put("id","order_amount");
        gmvMap.put("name","新增交易额");
        gmvMap.put("value",publisherService.getGmvAmountTotal(date));

        result.add(dauMap);
        result.add(devMap);
        result.add(gmvMap);

        return JSON.toJSONString(result);
    }
    @RequestMapping("realtime-hours")
    public String getDauHourTotal(@RequestParam("id")String id,
                                  @RequestParam("date")String date){
        //拿取service分时数据

        HashMap<String, Map> result = new HashMap<>();
        Map todayMap = null;
        Map yesterdayMap = null;
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        if(id.equals("dau")){
            todayMap = publisherService.getDauTotalHourMap(date);
            yesterdayMap = publisherService.getDauTotalHourMap(yesterday);
        }else if (id.equals("order_amount")){
            todayMap = publisherService.getGmvAmountHourMap(date);
            yesterdayMap = publisherService.getGmvAmountHourMap(yesterday);
        }

        //将数据保存至map集合
        result.put("yesterday",yesterdayMap);
        result.put("today",todayMap);

        return JSON.toJSONString(result);
    }
}
