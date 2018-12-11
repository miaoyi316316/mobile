package com.miao.logmobile.parser;


import com.miao.logmobile.common.KpiTypeEnum;
import com.miao.logmobile.parser.modle.dim.StatsBaseDimension;
import com.miao.logmobile.parser.modle.dim.base.KpiDimension;
import com.miao.logmobile.parser.modle.dim.keys.StatsLocationDimension;
import com.miao.logmobile.parser.modle.dim.keys.StatsUserDimension;
import com.miao.logmobile.parser.modle.dim.value.StatsBaseOutputDimension;
import com.miao.logmobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
import com.miao.logmobile.service.IDimensionInfo;
import com.miao.logmobile.service.JDBCService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.sql.*;
import java.util.Map;

public class ReduceOutputFormatImpl implements IReduceOutputFormat{

    private PreparedStatement ps;//用于插入会员表


    @Override
    public String buildInsertSql(KpiTypeEnum kpiTypeEnum, Configuration conf) {

        if(KpiTypeEnum.HOURLY_SESSIONS.equals(kpiTypeEnum)
                ||KpiTypeEnum.HOURLY_SESSIONS_LENGTH.equals(kpiTypeEnum)){
            return conf.get(KpiTypeEnum.HOURLY_ACTIVE_USER.getKpiType());

        }
        return conf.get(kpiTypeEnum.getKpiType());
    }

    @Override
    public void buildInsertPs(IDimensionInfo iDimensionInfo, StatsBaseDimension key, StatsBaseOutputDimension value, PreparedStatement ps) {

        if (key == null || value == null) {

            throw new RuntimeException("传入的key或者value为空");
        }





        ReduceOutputWritable realValue = (ReduceOutputWritable) value;

        KpiTypeEnum kpi = realValue.getKpiTypeName();
        if(KpiTypeEnum.LOCATION_ACTIVE_SESSION_LEAP.equals(kpi)){

            StatsLocationDimension realK = (StatsLocationDimension) key;

            int dateId = iDimensionInfo.getDimensionIdByDim(realK.getStatsCommonDimension().getDateDimension());

            int platFormId = iDimensionInfo.getDimensionIdByDim(realK.getStatsCommonDimension().getPlatFormDimension());

            iDimensionInfo.getDimensionIdByDim(new KpiDimension(kpi.getKpiType()));

            int locationId = iDimensionInfo.getDimensionIdByDim(realK.getLocationDimension());

            int activeUsers = ((IntWritable) realValue.getValue().get(new IntWritable(1))).get();
            int sessions = ((IntWritable) realValue.getValue().get(new IntWritable(2))).get();
            int bounce_sessions = ((IntWritable) realValue.getValue().get(new IntWritable(3))).get();

            int i = 0;
            try {
                ps.setInt(++i,dateId);
                ps.setInt(++i,platFormId);
                ps.setInt(++i,locationId);
                ps.setInt(++i,activeUsers);
                ps.setInt(++i,sessions);
                ps.setInt(++i,bounce_sessions);
                ps.setDate(++i,new Date(new java.util.Date().getTime()));
                ps.setInt(++i,activeUsers);
                ps.setInt(++i,sessions);
                ps.setInt(++i,bounce_sessions);
                ps.addBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return;

        }

        StatsUserDimension realKey = (StatsUserDimension) key;


        //公共维度
        int dateId = iDimensionInfo.getDimensionIdByDim(realKey.getStatsCommmonDimension().getDateDimension());

        int platFormId = iDimensionInfo.getDimensionIdByDim(realKey.getStatsCommmonDimension().getPlatFormDimension());

        iDimensionInfo.getDimensionIdByDim(new KpiDimension(kpi.getKpiType()));

        if (dateId == -1 || platFormId == -1) {
            throw new RuntimeException("dateId和platFormId为null，不能插入");
        }
        if (KpiTypeEnum.NEW_MEMBER.equals(kpi)||KpiTypeEnum.NEW_USER.equals(kpi) || KpiTypeEnum.ACTION_USER.equals(kpi)||KpiTypeEnum.ACTION_MEMBER.equals(kpi)) {
            int users = ((IntWritable) realValue.getValue().get(new IntWritable(-1))).get();
            //ps 赋值
            int i = 0;
            try {

                ps.setInt(++i, dateId);
                ps.setInt(++i, platFormId);
                ps.setInt(++i, users);
                ps.setDate(++i, new Date(new java.util.Date().getTime()));
                ps.setInt(++i, users);
                ps.addBatch();
                if(KpiTypeEnum.NEW_MEMBER.equals(kpi))
                    insertMemberInfo(realKey,realValue);
            } catch (SQLException e) {
                e.printStackTrace();
            }

        } else if (KpiTypeEnum.BROWSE_NEW_MEMBER.equals(kpi)||KpiTypeEnum.BROWSE_NEW_USER.equals(kpi) || KpiTypeEnum.ACTION_BROWSE_USER.equals(kpi)||KpiTypeEnum.BROWSE_ACTION_MEMBER.equals(kpi)) {

            int browserId = iDimensionInfo.getDimensionIdByDim(realKey.getBrowseDimension());

            if (browserId == -1) {
                throw new RuntimeException("browserId为null，不能插入");
            }
            int users = ((IntWritable) realValue.getValue().get(new IntWritable(-1))).get();
            //ps 赋值

            int i = 0;
            try {
                ps.setInt(++i, dateId);
                ps.setInt(++i, platFormId);
                ps.setInt(++i, browserId);
                ps.setInt(++i, users);
                ps.setDate(++i,new Date(new java.util.Date().getTime()));
                ps.setInt(++i, users);
                ps.addBatch();
                if(KpiTypeEnum.BROWSE_NEW_MEMBER.equals(kpi))
                    insertMemberInfo(realKey,realValue);
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }else if(KpiTypeEnum.SESSION.equals(kpi)){
            int sessionCount = ((IntWritable) realValue.getValue().get(new IntWritable(-1))).get();
            int sessionTime = ((IntWritable) realValue.getValue().get(new IntWritable(0))).get();
            int i = 0;
            try {
                ps.setInt(++i, dateId);
                ps.setInt(++i, platFormId);
                ps.setInt(++i, sessionCount);
                ps.setInt(++i, sessionTime);
                ps.setDate(++i, new Date(new java.util.Date().getTime()));
                ps.setInt(++i,sessionCount);
                ps.setInt(++i,sessionTime);
                ps.addBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }else if(KpiTypeEnum.BROWSE_SESSION.equals(kpi)){
            int browserId = iDimensionInfo.getDimensionIdByDim(realKey.getBrowseDimension());
            if(browserId==-1){
                throw new RuntimeException("browserId为null，不能插入");
            }
            int sessionCount = ((IntWritable) realValue.getValue().get(new IntWritable(-1))).get();
            int sessionTime = ((IntWritable) realValue.getValue().get(new IntWritable(0))).get();
            int i = 0;
            try {
                ps.setInt(++i, dateId);
                ps.setInt(++i, platFormId);
                ps.setInt(++i,browserId);
                ps.setInt(++i, sessionCount);
                ps.setInt(++i, sessionTime);
                ps.setDate(++i, new Date(new java.util.Date().getTime()));
                ps.setInt(++i,sessionCount);
                ps.setInt(++i,sessionTime);
                ps.addBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }else if(KpiTypeEnum.BRWOSE_PV.equals(kpi)){

            int browserId = iDimensionInfo.getDimensionIdByDim(realKey.getBrowseDimension());
            if(browserId==-1){
                throw new RuntimeException("browserId为null，不能插入");
            }
            int pvCount = ((IntWritable) realValue.getValue().get(new IntWritable(-1))).get();

            int i = 0;
            try {
                ps.setInt(++i, dateId);
                ps.setInt(++i, platFormId);
                ps.setInt(++i,browserId);
                ps.setInt(++i, pvCount);
                ps.setDate(++i, new Date(new java.util.Date().getTime()));
                ps.setInt(++i,pvCount);
                ps.addBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }else if(KpiTypeEnum.HOURLY_ACTIVE_USER.equals(kpi)||KpiTypeEnum.HOURLY_SESSIONS.equals(kpi)||KpiTypeEnum.HOURLY_SESSIONS_LENGTH.equals(kpi)){
            int kpiId = iDimensionInfo.getDimensionIdByDim(realKey.getStatsCommmonDimension().getKpiDimension());

            try {
                int i = 0;
                ps.setInt(++i,platFormId);
                ps.setInt(++i,dateId);
                ps.setInt(++i,kpiId);

                for (Map.Entry<Writable, Writable> entry : realValue.getValue().entrySet()) {

                    int hour = ((IntWritable) entry.getKey()).get();
                    int count= ((IntWritable) entry.getValue()).get();
                    ps.setInt(hour+4,count);

                    ps.setInt(hour+4+25,count);
                }
                ps.setDate(28,new Date(new java.util.Date().getTime()));

                ps.addBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }else {
            throw new RuntimeException("kpi类型不匹配");
        }

    }

    private void insertMemberInfo(StatsUserDimension key,ReduceOutputWritable value){
        Connection connection = JDBCService.getConnection();

        java.util.Date last_visit = key.getStatsCommmonDimension().getDateDimension().getCalendar();

        try {

            connection.setAutoCommit(false);

            if(this.ps==null||ps.isClosed()) {
                this.ps = connection.prepareStatement("insert " +
                        "into member_info(`member_id`,`last_visit_date`,`created`) values (?,?,?)" +
                        " on duplicate key update `created` = ?");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        String[] mids = ((Text) value.getValue().get(new IntWritable(0))).toString().split("\t");

        try {
            for(String mid:mids){
                this.ps.setString(1,mid);
                this.ps.setDate(2,new Date(last_visit.getTime()));
                this.ps.setDate(3,new Date(new java.util.Date().getTime()));
                this.ps.setDate(4,new Date(new java.util.Date().getTime()));
                this.ps.addBatch();
            }
            this.ps.executeBatch();
            connection.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


}
