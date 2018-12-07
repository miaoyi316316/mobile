package com.miao.logmobile.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {

    private static Properties properties = new Properties();

    static {
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("dimensionSql.properties");

        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String propertiesReadByKey(String key){

        if(key==null){
            return null;
        }
        return properties.getProperty(key);
    }
}
