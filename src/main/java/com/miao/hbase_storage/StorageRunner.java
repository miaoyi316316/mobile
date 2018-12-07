package com.miao.hbase_storage;

import com.miao.logmobile.etl.util.LogParser;

import java.util.Map;

public class StorageRunner {

    public static void main(String[] args) {
        MapToHbase mapToHbase = new MapToHbase();

//        Map<String, String> map = LogParser.getInfoMap("114.61.94.253^A1540454372.123^Ahh^A/BCImg.gif?en=e_l&ver=1&pl=website&sdk=js&u_ud=27F69684-BBE3-42FA-AA62-71F98E208444&u_mid=Aidon&u_sd=38F66FBB-C6D6-4C1C-8E05-72C31675C00A&c_time=1449917532123&l=zh-CN&b_iev=Mozilla%2F5.0%20(Windows%20NT%206.1%3B%20WOW64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F46.0.2490.71%20Safari%2F537.36&b_rst=1280*768");
//
//        HbaseUtil hbaseUtil = new HbaseUtil();
//
//        hbaseUtil.initNamespace("log_mobile");
//
//        hbaseUtil.createTable("log_mobile:logs", 4, "info");
//
//        mapToHbase.insertData(map,"log_mobile:logs");

        mapToHbase.getDataByDay("2017-05-31","log_mobile:logs");
    }
}
