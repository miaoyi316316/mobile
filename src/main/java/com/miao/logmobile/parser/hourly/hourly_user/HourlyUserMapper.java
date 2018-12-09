package com.miao.logmobile.parser.hourly.hourly_user;

import com.miao.logmobile.common.DateTypeEnum;
import com.miao.logmobile.common.KpiTypeEnum;
import com.miao.logmobile.parser.active_mr.ActiveUserMapper;
import com.miao.logmobile.parser.modle.dim.StatsCommonDimension;
import com.miao.logmobile.parser.modle.dim.base.BrowseDimension;
import com.miao.logmobile.parser.modle.dim.base.DateDimension;
import com.miao.logmobile.parser.modle.dim.base.KpiDimension;
import com.miao.logmobile.parser.modle.dim.base.PlatFormDimension;
import com.miao.logmobile.parser.modle.dim.keys.StatsUserDimension;
import com.miao.logmobile.parser.modle.dim.value.map.MapOutPutWritable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class HourlyUserMapper extends Mapper<LongWritable,Text,StatsUserDimension,MapOutPutWritable> {


    private static final Logger logger = Logger.getLogger(ActiveUserMapper.class);

    private long inputRecords = 0;
    private long filterRecords = 0;
    private long outputRecords = 0;

    private DateDimension dateDimension;
    private PlatFormDimension platFormDimension;
    private BrowseDimension browseDimension;
    private KpiDimension kpiDimension;
    private StatsCommonDimension statsCommonDimension;

    private MapWritable mapWritable;
    private StatsUserDimension outputKey;
    private MapOutPutWritable outputValue;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        inputRecords++;


        String[] dimensions = value.toString().split("\\u0001");


        //用户userId,用于封装outputValue
        outputValue = new MapOutPutWritable();
        String userId = dimensions[15];
        outputValue.setId(userId);
        //uid为null没要统计 直接pass
        if(StringUtils.isEmpty(userId)){
            filterRecords++;
            return;
        }

        //时间维度，用于封装outputkey中的dateDimension
        long timestamp = Long.valueOf(dimensions[1]);
        dateDimension = DateDimension.buildDate(timestamp, DateTypeEnum.DAY);


        //platform维度,用于封装outputKey中的platFormDimension
        String platFormName = dimensions[7];
        platFormDimension = new PlatFormDimension();
        platFormDimension.setPlatFormName(platFormName);


        //******************************stats_hourly中的活跃用户***********************
        kpiDimension = new KpiDimension(KpiTypeEnum.HOURLY_ACTIVE_USER.getKpiType());
        statsCommonDimension = new StatsCommonDimension(dateDimension, platFormDimension, kpiDimension);
        outputKey = new StatsUserDimension(statsCommonDimension, new BrowseDimension("", ""));
        outputValue = new MapOutPutWritable(userId,timestamp);
        context.write(outputKey,outputValue);
        outputRecords++;



    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.warn("inputRecords:"+inputRecords+"\n"+"filterRecords:"+filterRecords+"\n"+
                "outputRecord:"+outputRecords);
    }
}
