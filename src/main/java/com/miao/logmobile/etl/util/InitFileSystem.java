package com.miao.logmobile.etl.util;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.conf.*;
import org.apache.log4j.Logger;

import java.io.IOException;

public class InitFileSystem {

    private static final Logger logger = Logger.getLogger(InitFileSystem.class);

    public static FileSystem getFileSystem() {

        Configuration conf = new Configuration();

        FileSystem fs =  null;

        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            logger.warn("文件初始化失败",e);
        }

        return fs;

    }
}
