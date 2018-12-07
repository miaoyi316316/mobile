package com.miao.logmobile.parser.custom_outputformat;

import com.miao.logmobile.common.KpiTypeEnum;


import com.miao.logmobile.util.PropertiesUtil;


import java.sql.*;
import java.util.Map;

public class ResultFields {


    private static final int DEFAULT_VALUE = 0;


    /**
     * 根据指标不同 ，结合Properties配置文件进行sql语句的构建
     * @param reduceOutputKpi
     * @return
     */
    public static String[] createSqls(KpiTypeEnum reduceOutputKpi) {

        String selectSql = null;
        String insertSql = null;

        if(reduceOutputKpi.equals(KpiTypeEnum.BROWSE_NEW_USER)){
            selectSql = PropertiesUtil.propertiesReadByKey("sdb_selectOldUsersSql");
            insertSql = PropertiesUtil.propertiesReadByKey("sdb_insertSql");
        }else if(reduceOutputKpi.equals(KpiTypeEnum.NEW_USER)){
            selectSql = PropertiesUtil.propertiesReadByKey("su_selectOldUsersSql");
            insertSql = PropertiesUtil.propertiesReadByKey("su_insertSql");
        }else {
            return null;
        }
        return new String[]{selectSql, insertSql};

    }

    /**
     * 构建查询 preparedStatement 查询上一天的统计结果 ， 比若说 用户总量  会员总量之类的
     * @param ps
     * @param reduceOutputKpi
     * @param selectMap
     */
    public  static void buildSelectSqlForYesterday(PreparedStatement ps, KpiTypeEnum reduceOutputKpi, Map<String,Integer> selectMap) {

        int i = 0;
        if(reduceOutputKpi.equals(KpiTypeEnum.BROWSE_NEW_USER)){

            System.out.println(reduceOutputKpi.getKpiType());
            try {

                Integer value =  selectMap.get("platform_dimension_id");
                if(value==null){
                    throw new RuntimeException("查询昨天数据时指定的platform为空，中没有platform_dimension_id");
                }
                ps.setInt(++i,value);

                value = selectMap.get("browser_dimension_id");

                if(value==null){
                    throw new RuntimeException("该map中没有browser_dimension_id");
                }
                ps.setInt(++i,value);


            } catch (SQLException e) {
                e.printStackTrace();
            }
        }else if(reduceOutputKpi.equals(KpiTypeEnum.NEW_USER)){
            System.out.println(reduceOutputKpi.getKpiType());
            try {

                Integer value = selectMap.get("platform_dimension_id");
                if(value==null){
                    throw new RuntimeException("map中没有platform_dimension_id");
                }
                ps.setInt(++i,value);

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }else {
            throw new RuntimeException("构建查找昨天数据的sql失败，可能是该指标不存在");
        }
    }



    /**
     * 构建插入 preparedStatement
     * 根据指标不同，由于要插入的结果表不同，因此根据reduce传过来的Value中的MapWritable对象来赋值
     * @param ps
     * @param reduceOutputKpi 指标
     * @param insertMap  reduce端的value中的MapWritable中的键值对
     */
    public  static void buildInsertSql(PreparedStatement ps, KpiTypeEnum reduceOutputKpi,Map<String,Integer> insertMap ) {

        int i = 0;

        if(reduceOutputKpi.equals(KpiTypeEnum.BROWSE_NEW_USER)){

            try {

                ps.setInt(++i,insertMap.get("date_dimension_id"));


                ps.setInt(++i,insertMap.get("platform_dimension_id"));


                ps.setInt(++i,insertMap.get("browser_dimension_id"));


                ps.setInt(++i, insertMap.getOrDefault("new_install_users",DEFAULT_VALUE));


                ps.setInt(++i,insertMap.getOrDefault("total_install_users",DEFAULT_VALUE));


//                ps.setDate(++i,new Date(TimeTransform.String2long(TimeTransform.long2String(new java.util.Date().getTime()))));
            } catch (SQLException e) {
                e.printStackTrace();
            }

        } else if (reduceOutputKpi.equals(KpiTypeEnum.NEW_USER)) {

            try {

                ps.setInt(++i,insertMap.get("date_dimension_id"));


                ps.setInt(++i,insertMap.get("platform_dimension_id"));



                ps.setInt(++i, insertMap.getOrDefault("new_install_users",DEFAULT_VALUE));


                ps.setInt(++i,insertMap.getOrDefault("total_install_users",DEFAULT_VALUE));



//                ps.setDate(++i,new Date(TimeTransform.String2long(TimeTransform.long2String(new java.util.Date().getTime()))));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }else {
            throw new RuntimeException("该指标类型找不到");
        }

    }

//    public static void main(String[] args) {
//        Connection connection = JDBCService.getConnection();
//        MapWritable mapWritable = new MapWritable();
//        Text key = new Text();
//        IntWritable value = new IntWritable();
//        key.set("date_dimension_id");
//        value.set(1);
//        mapWritable.put(key, value);
//        key = new Text();
//        value = new IntWritable();
//        key.set("platform_dimension_id");
//        value.set(2);
//
//        mapWritable.put(key, value);
//        key = new Text();
//        value = new IntWritable();
//        key.set("new_install_users");
//        value.set(15);
//        mapWritable.put(key, value);
//        key = new Text();
//        value = new IntWritable();
//        key.set("total_install_users");
//        value.set(20);
//        mapWritable.put(key, value);
////        System.out.println(((IntWritable)(mapWritable.get(new Text("total_install_users")))).get());
//        ResultFields resultFields = new ResultFields();
//        resultFields.insertResult(connection, mapWritable,KpiTypeEnum.NEW_USER);
//    }
}
