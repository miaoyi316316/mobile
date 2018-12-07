package com.miao.logmobile.etl.util;


import com.miao.logmobile.common.LogFields;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import sun.nio.cs.US_ASCII;
import sun.rmi.runtime.Log;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class LogParser {



    private static final Logger logger = Logger.getLogger(LogParser.class);



//    private static void initInfoMap(){
//        infoMap = new LinkedHashMap<>();
//        infoMap.put(LogFields.LOG_IP, null);
//        infoMap.put(LogFields.LOG_SERVER_TIME, null);
//
//        infoMap.put(LogFields.LOG_EVENT, null);
//        infoMap.put(LogFields.LOG_VERSION, null);
//        infoMap.put(LogFields.LOG_PLATFORM, null);
//        infoMap.put(LogFields.LOG_SDK, null);
//        infoMap.put(LogFields.LOG_BROWSE_RESOLUTION, null);
//        infoMap.put(LogFields.LOG_USER_AGENT, null);
//
//        infoMap.put(LogFields.LOG_USER_ID, null);
//        infoMap.put(LogFields.LOG_LANGUAGE, null);
//        infoMap.put(LogFields.LOG_MEMBER_ID, null);
//        infoMap.put(LogFields.LOG_SESSION_ID, null);
//        infoMap.put(LogFields.LOG_CLIENT_TIME, null);
//        infoMap.put(LogFields.LOG_CURRENT_URL, null);
//        infoMap.put(LogFields.LOG_PREFIX_URL, null);
//        infoMap.put(LogFields.LOG_TITLE, null);
//
//
//        infoMap.put(LogFields.LOG_EVENT_CATEGORY_, null);
//        infoMap.put(LogFields.LOG_EVENT_ACTION, null);
//        infoMap.put(LogFields.LOG_EVENT_KEY_VALUE, null);
//        infoMap.put(LogFields.LOG_EVENT_DURATION, null);
//
//
//        infoMap.put(LogFields.LOG_ORDER_ID, null);
//        infoMap.put(LogFields.LOG_ORDER_NAME, null);
//        infoMap.put(LogFields.LOG_CURRENCY_AMOUNT, null);
//        infoMap.put(LogFields.LOG_CURRENCY_TYPE, null);
//        infoMap.put(LogFields.LOG_PAYMENT_TYPE, null);
//
//
//    }

    public static Map<String,String> getInfoMap(String log){

        if(StringUtils.isEmpty(log)){

            logger.warn("该日志为空");
            return null;
        }

        Map<String, String> infoMap = new ConcurrentHashMap<>();

        String[] infos = log.split(LogFields.LOG_SEPARATOR);

        infoMap.put(LogFields.LOG_IP, infos[0]);

        String serverTime = infos[1];
        if(StringUtils.isEmpty(serverTime)){
            return null;
        }
        infoMap.put(LogFields.LOG_SERVER_TIME, infos[1].replaceAll("\\.", ""));

        int index = infos[3].indexOf("?");
        String requestFields = null;
        if(index>0){
            requestFields = infos[3].substring(index + 1);
        }

        handlerRequest(infoMap,requestFields);

        handlerIp(infoMap);

        handlerUserAgent(infoMap);


        return infoMap;

    }

    /**
     * 将url请求切割开然后将对应的参数存放到map
     * @param map
     * @param fields
     */
    private static void handlerRequest(Map<String, String> map,String fields) {

        if(StringUtils.isEmpty(fields)){
            logger.warn("userAgent格式异常");
            return;
        }


        String[] kvs =fields.split("&");

        for(String kv:kvs){
            try {
                kv = URLDecoder.decode(kv, "utf-8");

            } catch (UnsupportedEncodingException e) {

                e.printStackTrace();

                throw new RuntimeException("url特殊符号解析异常");
            }

            String key = kv.split("=")[0];

            if(key!=null&&!key.trim().equals("")) {

                String value = kv.split("=")[1];

                map.put(key, value);

            }

        }

    }

    /**
     * 解析IP存入map
     * @param map
     */
    private static void handlerIp(Map<String,String> map){

        String ip = map.get(LogFields.LOG_IP);

        if(StringUtils.isEmpty(ip)){
            logger.warn("ip不存在,无法进行解析");
        }

        IpUtil.IpInfo ipInfo = IpUtil.getIpInfo(ip);

        map.put(LogFields.LOG_COUNTRY, ipInfo.getCountry());

        map.put(LogFields.LOG_PROVINCE, ipInfo.getProvince());

        map.put(LogFields.LOG_CITY, ipInfo.getCity());



    }

    /**
     * 解析userAgent然后放入map
     * @param map
     */
    private static void handlerUserAgent(Map<String, String> map) {

        String userAgent = map.get(LogFields.LOG_USER_AGENT);

        if(StringUtils.isEmpty(userAgent)){
            logger.warn("userAgent不存在,无法进行解析");
            return;
        }
        UserAgentUtil.UserAgentInfo userAgentInfo = UserAgentUtil.getUserAgentInfo(userAgent);

        map.put(LogFields.LOG_BROWSE_NAME, userAgentInfo.getBrowseName());
        map.put(LogFields.LOG_BROWSE_VERSION, userAgentInfo.getBrowseVersion());
        map.put(LogFields.LOG_OS_NAME, userAgentInfo.getOsName());
        map.put(LogFields.LOG_OS_VERSION, userAgentInfo.getOsVersion());


    }
}
