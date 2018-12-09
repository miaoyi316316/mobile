package com.miao.logmobile.parser.active_member_mr;

import com.miao.logmobile.common.DateTypeEnum;
import com.miao.logmobile.common.EventEnum;
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

public class ActiveMemberMapper extends Mapper<LongWritable,Text,StatsUserDimension,MapOutPutWritable> {


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


        EventEnum en = EventEnum.valuesOf(dimensions[5]);

        if (!EventEnum.PAGE_VIEW.equals(en)) {
            filterRecords++;
            return;
        }

        //会员mid,用于封装outputValue
        outputValue = new MapOutPutWritable();
        String memberId = dimensions[17];
        outputValue.setId(memberId);
        //mid为null没要统计 直接pass
        if(StringUtils.isEmpty(memberId)){
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


        //******************************stats_user中的active***********************
        kpiDimension = new KpiDimension(KpiTypeEnum.ACTION_MEMBER.getKpiType());
        statsCommonDimension = new StatsCommonDimension(dateDimension, platFormDimension, kpiDimension);
        outputKey = new StatsUserDimension(statsCommonDimension, new BrowseDimension("", ""));
        outputValue = new MapOutPutWritable(memberId);
        outputRecords++;
        context.write(outputKey,outputValue);

        //*******************************stats_device_browser中的acitve*********************
        //浏览器维度
        String browseName = dimensions[9];
        String browseVersion = dimensions[10];
        if (!StringUtils.isEmpty(browseName) && !StringUtils.isEmpty(browseVersion)) {
            browseDimension = new BrowseDimension(browseName, browseVersion);
            System.out.println(browseDimension.getBrowseName()+"***************mapper");
            kpiDimension = new KpiDimension(KpiTypeEnum.BROWSE_ACTION_MEMBER.getKpiType());
            statsCommonDimension = new StatsCommonDimension(dateDimension, platFormDimension, kpiDimension);
            outputKey = new StatsUserDimension(statsCommonDimension, browseDimension);
            outputValue = new MapOutPutWritable(memberId);
            outputRecords++;
            context.write(outputKey,outputValue);
        }




    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.warn("inputRecords:"+inputRecords+"\n"+"filterRecords:"+filterRecords+"\n"+
                "outputRecord:"+outputRecords);
    }
}
