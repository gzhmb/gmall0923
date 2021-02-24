package com.atguigu.dwpublisher.service.impl;

import com.atguigu.dwpublisher.mapper.DauMapper;
import com.atguigu.dwpublisher.mapper.OrderMapper;
import com.atguigu.dwpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;
    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {
        //获取数据
        HashMap<String, Long> result = new HashMap<>();
        List<Map> dauHourList = dauMapper.selectDauTotalHourMap(date);
        for (Map map : dauHourList) {
            result.put((String)map.get("LH"),(Long)map.get("CT"));
        }
        return result;
    }

    @Autowired
    private OrderMapper orderMapper;
    @Override
    public Double getGmvAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getGmvAmountHourMap(String date) {
        //获取数据
        HashMap<String, Long> result = new HashMap<>();
        List<Map> dauHourList = orderMapper.selectOrderAmountHourMap(date);
        for (Map map : dauHourList) {
            result.put((String)map.get("CREATE_HOUR"),(Long)map.get("SUM_AMOUNT"));
        }
        return result;
    }
}
