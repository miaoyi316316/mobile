package com.miao.hbase_storage;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DecimalFormat;

public class HbaseUtil {


    private static final Logger logger = Logger.getLogger(HbaseUtil.class);

    private static Connection connection;

    private  static Configuration conf ;

    private static Admin admin;

    /**
     * 如果链接为空或者被关闭了，重新创建connection
     */
    public HbaseUtil(){

        if(connection==null||connection.isClosed()) {

            conf = HBaseConfiguration.create();

            conf.addResource("hbase-site.xml");
            try {
                connection = ConnectionFactory.createConnection(conf);
                admin = connection.getAdmin();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public Connection getConnection() {
        return connection;
    }

    /**
     * 创建命名空间
     * @param namespace
     * @return
     */
    public boolean initNamespace(String namespace){

        try {
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;

    }

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     */
    public boolean tableExists(String tableName){

        TableName tableName1 = TableName.valueOf(tableName);
        try {
            return admin.tableExists(tableName1);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 根据指定的region分配策略创建表
     * @param tableName
     * @param regionNum
     * @param infos
     * @return
     */
    public boolean createTable(String tableName,int regionNum,String ...infos){
        TableName tb = TableName.valueOf(tableName);

        try {
            if(admin.tableExists(tb)){
                logger.warn("该表已存在，无法创建");
                return false;
            }
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tb);


            HColumnDescriptor hColumnDescriptor =null;

            for(String info:infos){

                hColumnDescriptor = new HColumnDescriptor(info);
                hTableDescriptor.addFamily(hColumnDescriptor);

            }
            byte[][] regionConf = setRegionConf(regionNum);

            admin.createTable(hTableDescriptor, regionConf);


        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 设置分区策略
     * @param regionNum
     * @return
     */
    private byte[][] setRegionConf(int regionNum) {

        int realNum = regionNum-1;
        byte[][] bytes = new byte[realNum][];
        DecimalFormat decimalFormat = new DecimalFormat("00");

        String[] splitRegion = new String[realNum];

        for(int i=0;i<splitRegion.length;i++){

            splitRegion[i] =decimalFormat.format(i)+"|";

            bytes[i] = Bytes.toBytes(splitRegion[i]);

        }

        return bytes;

    }


}
