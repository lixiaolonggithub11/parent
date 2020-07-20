package com.atguigu.gmall0105.publisher.service;

import java.util.List;
import java.util.Map;

public interface MySQLService {
    public List<Map> getTrademarkStat(String startTime, String endTime, int topn);
}
