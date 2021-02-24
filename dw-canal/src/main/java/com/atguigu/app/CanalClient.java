package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1获取连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111),
                "example", "", "");
        while(true){
            //2.获取连接
            canalConnector.connect();
            //3.指定监控的数据库
            canalConnector.subscribe("gmall.*");
            //4.获取数据
            Message message = canalConnector.get(100);
            //获取entry
            List<CanalEntry.Entry> entries = message.getEntries();
            if (entries.size()<=0){
                System.out.println("没有数据，休息一会");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                for (CanalEntry.Entry entry : entries) {
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    String tableName = entry.getHeader().getTableName();

                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)){
                        //获取序列化数据
                        ByteString storeValue = entry.getStoreValue();
                        //对数据进行反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        //TODO 获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //TODO 获取数据集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        handler(tableName,eventType,rowDatasList);
                    }
                }
            }
        }
    }
    private static void handler(String tableName,
                                CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //根据表名及事件类型获取数据
        if (tableName.equals("order_info") && CanalEntry.EventType.INSERT.equals(eventType)){
            for (CanalEntry.RowData rowData : rowDatasList) {
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {
                    //获取列名和列值
                    jsonObject.put(column.getName(),column.getValue());
                }
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER,jsonObject.toString());
            }
        }

    }
}
