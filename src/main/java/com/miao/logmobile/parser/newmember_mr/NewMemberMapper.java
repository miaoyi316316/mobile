package com.miao.logmobile.parser.newmember_mr;

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
import com.miao.logmobile.service.DimensionInfoImpl;
import com.miao.logmobile.service.IDimensionInfo;
import com.miao.logmobile.timeTransform.TimeTransform;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class NewMemberMapper extends Mapper<LongWritable,Text,StatsUserDimension,MapOutPutWritable> {

    private static final Logger logger = Logger.getLogger(NewMemberMapper.class);

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

    private Map<String,Integer> memberCache;

    private IDimensionInfo iDimensionInfo;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        iDimensionInfo= new DimensionInfoImpl();
        String todayTime = TimeTransform.long2String(new Date().getTime(), "yyyy-MM-dd");
        //先把会员表中今天的全删了
        iDimensionInfo.deleteOldMember(todayTime);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        inputRecords++;

        String[] dimensions = value.toString().split("\\u0001");

        //用户userId,用于封装outputValue
        outputValue = new MapOutPutWritable();
        String memberId = dimensions[17];
        outputValue.setId(memberId);
        //uid为null没要统计 直接pass
        if(StringUtils.isEmpty(memberId)){
            filterRecords++;
            return;
        }

        System.out.println(memberId+"++++++++++++++++");
        if(iDimensionInfo.isOldMember(memberId)){
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


        //******************************stats_user中的new member***********************
        kpiDimension = new KpiDimension(KpiTypeEnum.NEW_MEMBER.getKpiType());
        statsCommonDimension = new StatsCommonDimension(dateDimension, platFormDimension, kpiDimension);
        outputKey = new StatsUserDimension(statsCommonDimension, new BrowseDimension("", ""));
        outputValue = new MapOutPutWritable(memberId);
        context.write(outputKey,outputValue);
        outputRecords++;

        //*******************************stats_device_browser中的new member*********************
        //浏览器维度
        String browseName = dimensions[9];
        String browseVersion = dimensions[10];
        if (!StringUtils.isEmpty(browseName) && !StringUtils.isEmpty(browseVersion)) {
            browseDimension = new BrowseDimension(browseName, browseVersion);
            System.out.println(browseDimension.getBrowseName()+"***************mapper");
            kpiDimension = new KpiDimension(KpiTypeEnum.BROWSE_NEW_MEMBER.getKpiType());
            statsCommonDimension = new StatsCommonDimension(dateDimension, platFormDimension, kpiDimension);
            outputKey = new StatsUserDimension(statsCommonDimension, browseDimension);
            outputValue = new MapOutPutWritable(memberId);
            context.write(outputKey,outputValue);
            outputRecords++;
        }


    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.warn("inputRecords:"+inputRecords+"\n"+"filterRecords:"+filterRecords+"\n"+
                "outputRecord:"+outputRecords);
    }


}
