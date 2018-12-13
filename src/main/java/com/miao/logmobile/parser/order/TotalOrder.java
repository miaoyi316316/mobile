package com.miao.logmobile.parser.order;

import com.miao.logmobile.common.DateTypeEnum;
import com.miao.logmobile.common.LogFields;
import com.miao.logmobile.parser.modle.dim.base.DateDimension;
import com.miao.logmobile.service.DimensionInfoImpl;
import com.miao.logmobile.service.IDimensionInfo;
import com.miao.logmobile.service.JDBCService;
import com.miao.logmobile.timeTransform.TimeTransform;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class TotalOrder {


    public static void main(String[] args) {
        TotalOrder totalOrder = new TotalOrder();

        String date = null;

        for(int i=0;i<args.length;i++){
            if(args[i].equals("-d")){

                if(i+1<args.length){
                    date = args[i + 1];
                    break;
                }

            }
        }
        if(StringUtils.isEmpty(date)){

            date = TimeTransform.getYestarday();

        }
        totalOrder.totalOrder(date);

    }
    public void totalOrder(String date){

        IDimensionInfo iDimensionInfo = new DimensionInfoImpl();

        if(StringUtils.isEmpty(date)){

            date = TimeTransform.getYestarday();

        }

        DateDimension today = DateDimension.buildDate(TimeTransform.String2long(date, "yyyy-MM-dd"), DateTypeEnum.DAY);

        long yesterTime = (TimeTransform.getYesterDay(new Date(TimeTransform.String2long(date, "yyyy-MM-dd")), "yyyy-MM-dd")).getTime();
        DateDimension yester = DateDimension.buildDate(yesterTime, DateTypeEnum.DAY);

        int todayId = iDimensionInfo.getDimensionIdByDim(today);

        int yesterId = iDimensionInfo.getDimensionIdByDim(yester);

        Map<String, int[]> map = new ConcurrentHashMap<>();

        Connection connection = JDBCService.getConnection();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
             ps= connection.prepareStatement("select " +
                            "`platform_dimension_id`,`currency_type_dimension_id`,`payment_type_dimension_id`, `total_revenue_amount`,`total_refund_amount` " +
                            "from stats_order " +
                            "where `date_dimension_id` = ?"
                            );
             ps.setInt(1,yesterId);
            rs = ps.executeQuery();

            while (rs.next()){
                int platform_dimension_id = rs.getInt("platform_dimension_id");
                int currency_type_dimension_id = rs.getInt("currency_type_dimension_id");
                int payment_type_dimension_id = rs.getInt("payment_type_dimension_id");
                int total_revenue_amount = rs.getInt("total_revenue_amount");
                int total_refund_amount = rs.getInt("total_refund_amount");

                map.put(platform_dimension_id +"_"+ currency_type_dimension_id +"_"+ payment_type_dimension_id,new int[]{total_revenue_amount,total_refund_amount});

            }
            ps= connection.prepareStatement("select " +
                    "`platform_dimension_id`,`currency_type_dimension_id`,`payment_type_dimension_id`,`revenue_amount`,`refund_amount` " +
                    "from stats_order " +
                    "where `date_dimension_id` = ?"
            );
            ps.setInt(1,todayId);
            rs = ps.executeQuery();
            while (rs.next()){
                int platform_dimension_id = rs.getInt("platform_dimension_id");
                int currency_type_dimension_id = rs.getInt("currency_type_dimension_id");
                int payment_type_dimension_id = rs.getInt("payment_type_dimension_id");
                int revenue_amount = rs.getInt("revenue_amount");
                int refund_amount = rs.getInt("refund_amount");

                if(map.containsKey(platform_dimension_id + currency_type_dimension_id + payment_type_dimension_id + "")){
                    int[] total = map.get(platform_dimension_id + currency_type_dimension_id + payment_type_dimension_id + "");

                    revenue_amount += total[0];
                    refund_amount += total[1];
                }

                map.put(platform_dimension_id +"_"+ currency_type_dimension_id +"_"+ payment_type_dimension_id , new int[]{revenue_amount, refund_amount});

            }

            connection.setAutoCommit(false);
            ps = connection.prepareStatement("insert into stats_order(" +
                    "`platform_dimension_id`," +
                    "`date_dimension_id`," +
                    "`currency_type_dimension_id`," +
                    "`payment_type_dimension_id`," +
                    "`total_revenue_amount`," +
                    "`total_refund_amount`," +
                    "`created`) values(?,?,?,?,?,?,?) " +
                    "on duplicate key update `total_revenue_amount`=?,`total_refund_amount`=?,`created`=?");

            for(Map.Entry<String,int[]> entry:map.entrySet()){
                int i = 0;
                String[] info = entry.getKey().split("_");

                ps.setInt(++i,Integer.valueOf(info[0]));
                ps.setInt(++i,todayId);
                ps.setInt(++i,Integer.valueOf(info[1]));
                ps.setInt(++i,Integer.valueOf(info[2]));
                ps.setInt(++i,entry.getValue()[0]);
                ps.setInt(++i,entry.getValue()[1]);
                ps.setDate(++i,new java.sql.Date(new Date().getTime()));
                ps.setInt(++i,entry.getValue()[0]);
                ps.setInt(++i,entry.getValue()[1]);
                ps.setDate(++i,new java.sql.Date(new Date().getTime()));
                ps.addBatch();
            }
            ps.executeBatch();
            connection.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JDBCService.colseAll(connection,ps,rs);
        }


    }


}
