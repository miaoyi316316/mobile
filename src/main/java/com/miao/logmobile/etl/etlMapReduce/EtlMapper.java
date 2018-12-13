package com.miao.logmobile.etl.etlMapReduce;

import com.miao.hbase_storage.HbaseUtil;
import com.miao.hbase_storage.MapToHbase;
import com.miao.logmobile.common.EventEnum;
import com.miao.logmobile.common.LogFields;
import com.miao.logmobile.etl.etlDimension.LogDimension;
import com.miao.logmobile.etl.util.LogParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;


public class EtlMapper extends Mapper<LongWritable,Text,LogDimension,NullWritable> {

    private static final Logger logger = Logger.getLogger(EtlMapper.class);
    private LogDimension outputKey ;

    private Map<String,String> map ;

    private int inputRecord,outputRecord, filterRecoder = 0;


    private MapToHbase mapToHbase;

    private  HbaseUtil hbaseUtil;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        mapToHbase = new MapToHbase();
//
//        hbaseUtil = new HbaseUtil();
//
//        if (!hbaseUtil.tableExists("log_mobile:logs")) {
//            hbaseUtil.createTable("log_mobile:logs", 4, "info");
//        }

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        inputRecord++;
        String log = value.toString();

        map = LogParser.getInfoMap(log,context.getConfiguration());

        if(map==null){
            filterRecoder++;
            return;
        }
        EventEnum eventEnum = EventEnum.valuesOf(map.get(LogFields.LOG_EVENT));

        //按照事件类型进行匹配，没有触发事件的可以不要；

        if(eventEnum==null){
            filterRecoder++;
         return;
        }
        outputKey = new LogDimension();
        switch (eventEnum){
            case LAUNCH:
            case EVENT:
            case PAGE_VIEW:
            case CHARGE_REFUND:
            case CHARGE_REQUEST:
            case CHARGE_SUCCESS:
            {
                outputKey.setLog_event(map.getOrDefault(LogFields.LOG_EVENT ,""));
                outputKey.setLog_version( map.getOrDefault(LogFields.LOG_VERSION ,""));
                outputKey.setLog_platform( map.getOrDefault(LogFields.LOG_PLATFORM  ,""));
                outputKey.setLog_sdk( map.getOrDefault(LogFields.LOG_SDK  ,""));
                outputKey.setLog_browse_resolution( map.getOrDefault(LogFields.LOG_BROWSE_RESOLUTION  ,""));
                outputKey.setLog_user_agent( map.getOrDefault(LogFields.LOG_USER_AGENT  ,""));
                outputKey.setLog_user_id( map.getOrDefault(LogFields.LOG_USER_ID  ,""));
                outputKey.setLog_language( map.getOrDefault(LogFields.LOG_LANGUAGE  ,""));
                outputKey.setLog_member_id( map.getOrDefault(LogFields.LOG_MEMBER_ID,""));
                outputKey.setLog_session_id( map.getOrDefault(LogFields.LOG_SESSION_ID ,""));
                outputKey.setLog_client_time( map.getOrDefault(LogFields.LOG_CLIENT_TIME,""));
                outputKey.setLog_current_url( map.getOrDefault(LogFields.LOG_CURRENT_URL,""));
                outputKey.setLog_prefix_url( map.getOrDefault(LogFields.LOG_PREFIX_URL  ,""));
                outputKey.setLog_title( map.getOrDefault(LogFields.LOG_TITLE  ,""));
                outputKey.setLog_event_category( map.getOrDefault(LogFields.LOG_EVENT_CATEGORY  ,""));
                outputKey.setLog_event_action( map.getOrDefault(LogFields.LOG_EVENT_ACTION  ,""));
                outputKey.setLog_event_key_value( map.getOrDefault(LogFields.LOG_EVENT_KEY_VALUE  ,""));
                outputKey.setLog_event_duration( map.getOrDefault(LogFields.LOG_EVENT_DURATION  ,""));
                outputKey.setLog_order_id( map.getOrDefault(LogFields.LOG_ORDER_ID  ,""));
                outputKey.setLog_order_name( map.getOrDefault(LogFields.LOG_ORDER_NAME  ,""));
                outputKey.setLog_currency_amount( map.getOrDefault(LogFields.LOG_CURRENCY_AMOUNT  ,""));
                outputKey.setLog_currency_type( map.getOrDefault(LogFields.LOG_CURRENCY_TYPE  ,""));
                outputKey.setLog_payment_type( map.getOrDefault(LogFields.LOG_PAYMENT_TYPE  ,""));
                outputKey.setLog_ip( map.getOrDefault(LogFields.LOG_IP  ,""));
                outputKey.setLog_server_time( map.getOrDefault(LogFields.LOG_SERVER_TIME  ,""));
                outputKey.setLog_country( map.getOrDefault(LogFields.LOG_COUNTRY  ,""));
                outputKey.setLog_province( map.getOrDefault(LogFields.LOG_PROVINCE  ,""));
                outputKey.setLog_city( map.getOrDefault(LogFields.LOG_CITY  ,""));
                outputKey.setLog_browse_name(map.getOrDefault(LogFields.LOG_BROWSE_NAME,""));
                outputKey.setLog_browse_version(map.getOrDefault(LogFields.LOG_BROWSE_VERSION,""));
                outputKey.setLog_os_name(map.getOrDefault(LogFields.LOG_OS_NAME,""));
                outputKey.setLog_os_version(map.getOrDefault(LogFields.LOG_OS_VERSION,""));
                context.write(outputKey,NullWritable.get());

                outputRecord++;

//                mapToHbase.insertData(map,"log_mobile:logs");
                break;
            }
            default:{
                filterRecoder++;
            }
        }

    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("\ninputRecoder:"+inputRecord+"\n"+"outputRecoder:"+outputRecord+"\n"+
                "filterRecoder: "+filterRecoder);

    }
}
