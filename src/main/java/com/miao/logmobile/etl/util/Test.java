package com.miao.logmobile.etl.util;

import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Test {

    public static void main(String[] args) {

        long time = 1499154588619l;

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        Date date = new Date(time);

        System.out.println(simpleDateFormat.format(date));
    }
}
