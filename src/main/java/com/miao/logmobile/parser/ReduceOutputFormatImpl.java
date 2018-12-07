package com.miao.logmobile.parser;


import com.miao.logmobile.common.KpiTypeEnum;
import com.miao.logmobile.parser.modle.dim.StatsBaseDimension;
import com.miao.logmobile.parser.modle.dim.keys.StatsUserDimension;
import com.miao.logmobile.parser.modle.dim.value.StatsBaseOutputDimension;
import com.miao.logmobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
import com.miao.logmobile.service.IDimensionInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ReduceOutputFormatImpl implements IReduceOutputFormat{


    @Override
    public String buildInsertSql(KpiTypeEnum kpiTypeEnum, Configuration conf) {

        if(KpiTypeEnum.NEW_USER.equals(kpiTypeEnum)){

            return conf.get("insert_new_user");
        }else if(KpiTypeEnum.BROWSE_NEW_USER.equals(kpiTypeEnum)){
            return conf.get("insert_browser_new_user");
        }

        return null;
    }

    @Override
    public void buildInsertPs(IDimensionInfo iDimensionInfo,StatsBaseDimension key, StatsBaseOutputDimension value, PreparedStatement ps) {

        if(key==null||value==null){

            throw new RuntimeException("传入的key或者value为kong");
        }

        StatsUserDimension realKey = (StatsUserDimension) key;

        ReduceOutputWritable realValue = (ReduceOutputWritable) value;

        KpiTypeEnum kpi = realValue.getKpiTypeName();

        //公共维度
        int dateId = iDimensionInfo.getDimensionIdByDim(realKey.getStatsCommmonDimension().getDateDimension());

        int platFormId = iDimensionInfo.getDimensionIdByDim(realKey.getStatsCommmonDimension().getPlatFormDimension());

        if(dateId==-1||platFormId==-1){
            throw new RuntimeException("dateId和platFormId为null，不能插入");
        }
        if(KpiTypeEnum.NEW_USER.equals(kpi)){
            int new_install_users = ((IntWritable)realValue.getValue().get(new Text(kpi.getKpiType()))).get();
            //ps 赋值

            System.out.println(dateId+" "+platFormId+" "+new_install_users);
            int i = 0;
            try {
                ps.setInt(++i,dateId);
                ps.setInt(++i,platFormId);
                ps.setInt(++i,new_install_users);
                ps.setDate(++i,new Date(new java.util.Date().getTime()));
                ps.setInt(++i,new_install_users);
                ps.addBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }else if(KpiTypeEnum.BROWSE_NEW_USER.equals(kpi)){

            int browserId = iDimensionInfo.getDimensionIdByDim(realKey.getBrowseDimension());
            if(browserId==-1){
                throw new RuntimeException("browserId为null，不能插入");
            }
            int new_install_users = ((IntWritable)realValue.getValue().get(new Text(kpi.getKpiType()))).get();
            //ps 赋值

            int i = 0;
            try {
                ps.setInt(++i,dateId);
                ps.setInt(++i,platFormId);
                ps.setInt(++i,browserId);
                ps.setInt(++i,new_install_users);
                ps.setDate(++i,new Date(new java.util.Date().getTime()));
                ps.setInt(++i,new_install_users);
                ps.addBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        } else if (KpiTypeEnum.NEW_ALL_USER.equals(kpi)) {

        }

    }

    @Override
    public void buildSelectPs(IDimensionInfo iDimensionInfo, StatsBaseDimension key, StatsBaseOutputDimension value, PreparedStatement ps) {



    }

    @Override
    public String buildSelectSql(KpiTypeEnum kpiTypeEnum, Configuration conf) {
        return null;
    }
}
