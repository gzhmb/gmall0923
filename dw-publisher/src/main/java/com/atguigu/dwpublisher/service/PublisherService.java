package com.atguigu.dwpublisher.service;

import java.util.List;
import java.util.Map;

public interface PublisherService {
    //日活总数
    public Integer getDauTotal(String date);

    public Map getDauTotalHourMap(String date);

    public Double getGmvAmountTotal(String date);

    public Map getGmvAmountHourMap(String date);
}
