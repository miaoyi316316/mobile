package com.miao.logmobile.service;

import com.miao.logmobile.common.DateTypeEnum;
import com.miao.logmobile.parser.modle.dim.base.*;
import com.miao.logmobile.util.PropertiesUtil;

import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

public class DimensionInfoImpl implements IDimensionInfo {

    private static final int MAX_SIZE = 5000;

    private Connection connection = JDBCService.getConnection();

    private PreparedStatement ps;

    private ResultSet rs;


    private Map<String, Integer> cacheMap = new LinkedHashMap<String,Integer>(){
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return this.size()>MAX_SIZE;
        }
    };

    /**
     * 通过指定维度获得到其在维度表中的id
     * @param dimension
     * @return
     */
    @Override
    public int getDimensionIdByDim(BaseDimension dimension) {

        String cacheKey = buildCacheKey(dimension);

        if(cacheKey==null){
            throw new RuntimeException("构建cacheKey出现异常，该维度可能不存在");
        }
        int id = cacheMap.getOrDefault(cacheKey,-1);

        if(id==-1) {

            //加个同步锁防止多个线程同时插入造成数据不一致
            synchronized (DimensionInfoImpl.class) {
                if (connection == null) {
                    connection = JDBCService.getConnection();
                    System.out.println(connection);
                }

                String[] sqls = buildSqls(dimension);
                if (sqls == null) {
                    throw new RuntimeException("sql 语句初始化异常，该维度可能不存在");
                }
                //从数据库中查询
                id = execSql(sqls, connection, dimension);

                if (id != -1) {
                    cacheMap.put(cacheKey, id);
                    return id;
                }
            }
        }

        return id;
    }


    /**
     * 执行维度表的插入和查询等语句
     * @param sqls
     * @param connection
     * @return
     */
    private  int execSql(String[] sqls, Connection connection,BaseDimension dimension) {
        int id = -1;
        //cache查不到，先去查找数据库
        try {
            ps = connection.prepareStatement(sqls[0]);
            createPs(ps,dimension);
            rs = ps.executeQuery();
            if(rs.next()){
                id = rs.getInt(1);
                return id;
            }

            ps = connection.prepareStatement(sqls[1], Statement.RETURN_GENERATED_KEYS);
            createPs(ps, dimension);
            ps.executeUpdate();
            rs = ps.getGeneratedKeys();
            if (rs.next()) {
                id = rs.getInt(1);
                return id;
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }finally {
            JDBCService.colseAll(null,ps,rs);
        }


        return id;
    }

    /**
     * 构建preparedStatement
     * @param ps
     * @param dimension
     */
    private void createPs(PreparedStatement ps, BaseDimension dimension) {
        int i=0;
        if(dimension instanceof BrowseDimension){
            BrowseDimension browseDimension = (BrowseDimension) dimension;

            try {
                ps.setString(++i,browseDimension.getBrowseName());
                ps.setString(++i,browseDimension.getBrowseVersion());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }else if(dimension instanceof PlatFormDimension){
            PlatFormDimension platFormDimension = (PlatFormDimension) dimension;
            try {
                ps.setString(++i,platFormDimension.getPlatFormName());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }else if(dimension instanceof KpiDimension){
            KpiDimension kpiDimension = (KpiDimension) dimension;
            try {
                ps.setString(++i,kpiDimension.getKpiName());
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }else if(dimension instanceof DateDimension){
            DateDimension dateDimension = (DateDimension) dimension;
            try {
                ps.setInt(++i,dateDimension.getYear());
                ps.setInt(++i,dateDimension.getSeason());
                ps.setInt(++i,dateDimension.getMonth());
                ps.setInt(++i,dateDimension.getWeek());
                ps.setInt(++i,dateDimension.getDay());
                ps.setDate(++i,new Date(dateDimension.getCalendar().getTime()));
                ps.setString(++i,dateDimension.getType());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }else {
            throw new RuntimeException("preparedStatement构建失败，可能该维度不存在");
        }


    }

    /**
     * 根据dimension封装sql语句  0是查询 1是插入
     * @param dimension
     * @return
     */
    private String[] buildSqls(BaseDimension dimension) {

        String selectSql= null;
        String insertSql = null;

        if(dimension instanceof BrowseDimension){

            selectSql = PropertiesUtil.propertiesReadByKey("br_selectSql");
            insertSql = PropertiesUtil.propertiesReadByKey("br_insertSql");
//            selectSql = "select `id` from dimension_browser where `browser_name` = ? and `browser_version`=? limit 1";
//
//            insertSql = "insert into dimension_browser(`browser_name`,`browser_version`) values(?,?)";
        }else if(dimension instanceof PlatFormDimension){
            selectSql = PropertiesUtil.propertiesReadByKey("pl_selectSql");
            insertSql = PropertiesUtil.propertiesReadByKey("pl_insertSql");

//            selectSql = "select `id` from dimension_platform where `platform_name` =? limit 1";
//            insertSql = "insert into dimension_platform(`platform_name`) values(?)";
        }else if(dimension instanceof KpiDimension){
            selectSql = PropertiesUtil.propertiesReadByKey("kpi_selectSql");
            insertSql = PropertiesUtil.propertiesReadByKey("kpi_insertSql");

//            selectSql = "select `id` from dimension_kpi where `kpi_name` =? limit 1";
//            insertSql = "insert into dimension_kpi(`kpi_name`) values(?)";
        }else if(dimension instanceof DateDimension){

            selectSql = PropertiesUtil.propertiesReadByKey("dt_selectSql");
            insertSql = PropertiesUtil.propertiesReadByKey("dt_insertSql");
//            selectSql = "select `id` from dimension_date where `year` =? and `season` = ? " +
//                    " and `month` = ? and `week` = ? and `day` = ? and `calendar` = ? " +
//                    " and type = ? limit 1";
//
//            insertSql = "insert into dimension_date(`year`,`season`,`month`,`week`,`day`," +
//                    "`calendar`,`type`) values(?,?,?,?,?,?,?)";

        }else {
            return null;
        }

        return new String[]{selectSql, insertSql};
    }

    /**
     * 根据不同的dimension建立cachekey，用作缓冲map的key
     * @param dimension
     * @return
     */
    private String buildCacheKey(BaseDimension dimension) {

        StringBuilder cacheKey = new StringBuilder();

        if(dimension instanceof BrowseDimension){
            BrowseDimension browseDimension = (BrowseDimension) dimension;
            cacheKey.append("br_")
                    .append(browseDimension.getBrowseName()+"_")
                    .append(browseDimension.getBrowseVersion());
        }else if(dimension instanceof PlatFormDimension){
            PlatFormDimension platFormDimension = (PlatFormDimension) dimension;
            cacheKey.append("pl_")
                    .append(platFormDimension.getPlatFormName());
        }else if(dimension instanceof DateDimension){
            DateDimension dateDimension = (DateDimension) dimension;
            cacheKey.append("dt_")
                    .append(dateDimension.getYear() + "_")
                    .append(dateDimension.getSeason() + "_")
                    .append(dateDimension.getMonth() + "_")
                    .append(dateDimension.getWeek() + "_")
                    .append(dateDimension.getDay() + "_")
                    .append(dateDimension.getType());
        }else if(dimension instanceof KpiDimension){
            KpiDimension kpiDimension = (KpiDimension) dimension;
            cacheKey.append("kpi_")
                    .append(kpiDimension.getKpiName());

        }else {
            return null;
        }

        return cacheKey.toString();
    }



}
