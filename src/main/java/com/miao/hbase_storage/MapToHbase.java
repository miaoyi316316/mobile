package com.miao.hbase_storage;

import com.miao.logmobile.common.LogFields;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class MapToHbase {

    private HbaseUtil hbase_connect = new HbaseUtil();

    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    private DecimalFormat decimalFormat = new DecimalFormat("00");

    private static final Logger logger = Logger.getLogger(MapToHbase.class);

    private Table table;

    /**
     * 初始化表
     * @param tableName
     */
    private void initTable(String tableName){
        if(table==null){
            try {
                if(hbase_connect.tableExists(tableName))
                    table = hbase_connect.getConnection().getTable(TableName.valueOf(tableName));
                else {
                    throw new RuntimeException("表不存在，无法获得");
                }
            } catch (IOException e) {
                throw new RuntimeException("获得表失败");
            }
        }
    }

    /**
     * 将map中的数据插入到指定的hbase表
     * @param map
     * @param tableName
     */
    public void insertData(Map<String,String> map,String tableName){

       if(map==null){
           logger.warn("map 为空，无法插入");
           return;
       }

       String timestamp = map.get(LogFields.LOG_SERVER_TIME);

       String uid = map.get(LogFields.LOG_USER_ID);

       String rowkey = setRowkey(timestamp, 4, uid);

       if(StringUtils.isEmpty(rowkey)){
           logger.warn("rowkey生成失败 ，跳过本条数据");
           return;
       }

        initTable(tableName);

        Put put = new Put(Bytes.toBytes(rowkey));

        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_ip"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_IP,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_server_time"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_SERVER_TIME,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_country"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_COUNTRY,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_province"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_PROVINCE,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_city"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_CITY,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_event"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_EVENT,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_version"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_VERSION,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_platform"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_PLATFORM,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_sdk"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_SDK,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_browse_resolution"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_BROWSE_RESOLUTION,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_user_agent"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_USER_AGENT,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_user_id"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_USER_ID,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_language"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_LANGUAGE,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_member_id"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_MEMBER_ID,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_session_id"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_SESSION_ID,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_client_time"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_CLIENT_TIME,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_current_url"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_CURRENT_URL,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_prefix_url"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_PREFIX_URL,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_title"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_TITLE,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_event_category"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_EVENT_CATEGORY,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_event_action"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_EVENT_ACTION,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_event_key_value"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_EVENT_KEY_VALUE,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_event_duration"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_EVENT_DURATION,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_order_id"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_ORDER_ID,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_order_name"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_ORDER_NAME,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_currency_amount"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_CURRENCY_AMOUNT,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_currency_type"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_CURRENCY_TYPE,"")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("log_payment_type"),Bytes.toBytes(map.getOrDefault(LogFields.LOG_PAYMENT_TYPE,"")));

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    /**
     * 根据天查看hbase中的数据
     * @param day
     * @param tableName
     */
    public void getDataByDay(String day,String tableName){

        if(StringUtils.isEmpty(day)){
            logger.warn("输入日期为空");
            return;
        }
        day = day.replaceAll("-", "");

        String startDate = day + "0";

        String stopDate = day + "|";

        String pre = decimalFormat.format(Integer.valueOf(day) % 4);
        String startKey =pre+"_"+startDate;

        String stopKey = pre + "_" + stopDate;

        initTable(tableName);

        Scan scan = new Scan(Bytes.toBytes(startKey), Bytes.toBytes(stopKey));
        ResultScanner resultScanner = null;
        try {

            resultScanner = table.getScanner(scan);

            for(Result rs:resultScanner){
                Cell[] cells =rs.rawCells();
                System.out.print(Bytes.toString(CellUtil.cloneRow(cells[0]))+"\t");
                System.out.print(Bytes.toString(CellUtil.cloneFamily(cells[0]))+"\t");
                for(Cell cell:cells){

                    System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell))+"\t");

                    System.out.print(Bytes.toString(CellUtil.cloneValue(cell))+"\t");
                }
                System.out.println();
            }




        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    /**
     * 根据时间戳  转化成 year-month-day 的格式，然后把他们”-“去掉拼在一起，对regionNum求余
     * @param timestamp
     * @param regionNum
     * @return
     */
   private String setRowkey(String timestamp,int regionNum,String uid){

       Date date = new Date(Long.valueOf(timestamp));

       String formatDate = simpleDateFormat.format(date);

       int index = formatDate.indexOf("-");

       if(index>0){
           int year_month_day = Integer.valueOf(formatDate.replaceAll("-", ""));

           String  prefix = decimalFormat.format(year_month_day % 4);

           return prefix+"_"+year_month_day+"_"+uid;
       }
       return null;

   }





}
