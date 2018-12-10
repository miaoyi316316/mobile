package com.miao.logmobile.parser.location_mr;

import com.miao.logmobile.common.DateTypeEnum;
import com.miao.logmobile.common.KpiTypeEnum;
import com.miao.logmobile.parser.active_mr.ActiveUserMapper;
import com.miao.logmobile.parser.modle.dim.StatsCommonDimension;
import com.miao.logmobile.parser.modle.dim.base.*;
import com.miao.logmobile.parser.modle.dim.keys.StatsLocationDimension;
import com.miao.logmobile.parser.modle.dim.keys.StatsUserDimension;
import com.miao.logmobile.parser.modle.dim.value.map.MapOutPutWritable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class LocationMapper extends Mapper<LongWritable,Text,StatsLocationDimension,MapOutPutWritable> {

    private static final Logger logger = Logger.getLogger(ActiveUserMapper.class);

    private long inputRecords = 0;
    private long filterRecords = 0;
    private long outputRecords = 0;

    private DateDimension dateDimension;
    private PlatFormDimension platFormDimension;
    private KpiDimension kpiDimension;
    private LocationDimension locationDimension;
    private StatsCommonDimension statsCommonDimension;

    private MapWritable mapWritable;
    private StatsLocationDimension outputKey;
    private MapOutPutWritable outputValue;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        inputRecords++;


        String[] dimensions = value.toString().split("\\u0001");


        //用户userId,用于封装outputValue
        outputValue = new MapOutPutWritable();
        String userId = dimensions[15];

        //uid为null没要统计 直接pass
        if(StringUtils.isEmpty(userId)){
            filterRecords++;
            return;
        }
        String sessionId = dimensions[18];
        //sessionId为null没要统计 直接pass
        if(StringUtils.isEmpty(sessionId)){
            filterRecords++;
            return;
        }

        //获得session的建立时间
        String sessionTime = dimensions[19];
        if(StringUtils.isEmpty(sessionTime)){
            filterRecords++;
            return;
        }
        outputValue.setId(userId+":"+sessionId);
        outputValue.setSessionTime(Long.valueOf(sessionTime));

        String country = dimensions[2];
        String province = dimensions[3];
        String city = dimensions[4];


        //时间维度，用于封装outputkey中的dateDimension
        long timestamp = Long.valueOf(dimensions[1]);
        dateDimension = DateDimension.buildDate(timestamp, DateTypeEnum.DAY);


        //platform维度,用于封装outputKey中的platFormDimension
        String platFormName = dimensions[7];
        platFormDimension = new PlatFormDimension();
        platFormDimension.setPlatFormName(platFormName);


        //******************************国家级别all平台***********************
        kpiDimension = new KpiDimension(KpiTypeEnum.LOCATION_ACTIVE_SESSION_LEAP.getKpiType());
        statsCommonDimension = new StatsCommonDimension(dateDimension,new PlatFormDimension(""), kpiDimension);
        locationDimension = new LocationDimension(country, "", "");
        outputKey = new StatsLocationDimension(statsCommonDimension, locationDimension);
        context.write(outputKey,outputValue);





    }
}
