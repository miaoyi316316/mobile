package com.miao.logmobile.parser.newuser_mr;

import com.miao.logmobile.common.DateTypeEnum;
import com.miao.logmobile.common.EventEnum;
import com.miao.logmobile.common.KpiTypeEnum;
import com.miao.logmobile.parser.modle.dim.StatsCommonDimension;
import com.miao.logmobile.parser.modle.dim.base.BrowseDimension;
import com.miao.logmobile.parser.modle.dim.base.DateDimension;
import com.miao.logmobile.parser.modle.dim.base.KpiDimension;
import com.miao.logmobile.parser.modle.dim.base.PlatFormDimension;
import com.miao.logmobile.parser.modle.dim.keys.StatsUserDimension;
import com.miao.logmobile.parser.modle.dim.value.map.MapOutPutWritable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class NewUserValueMapper extends Mapper<LongWritable,Text,StatsUserDimension,MapOutPutWritable> {

    private static final Logger logger = Logger.getLogger(NewUserValueMapper.class);

    private long inputRecord = 0;
    private long outputRecord = 0;
    private long filterRecord = 0;//不是lanunch事件,或者某些必须指标为空

    //map端输出 kv
    private StatsUserDimension outputKey;
    private MapOutPutWritable outputValue;

    //各个维度
    private DateDimension dateDimension;
    private BrowseDimension browseDimension;
    private KpiDimension kpiDimension;
    private PlatFormDimension platFormDimension;

    //公用维度
    private StatsCommonDimension statsCommonDimension;


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        inputRecord++;

        System.out.println(value.toString() + "*********");
        String[] dimensions = value.toString().split("\\u0001");


        //event事件判断，不是launch事件的直接pass
        EventEnum en = EventEnum.valuesOf(dimensions[5]);
        if (!EventEnum.LAUNCH.equals(en)) {
            filterRecord++;
            return;
        }

        //用户userId,用于封装outputValue
        outputValue = new MapOutPutWritable();
        String userId = dimensions[15];
        outputValue.setId(userId);
        //uid为null没要统计 直接pass
        if(StringUtils.isEmpty(userId)){
            filterRecord++;
            return;
        }

        //时间维度，用于封装outputkey中的dateDimension
        long timestamp = Long.valueOf(dimensions[1]);
        dateDimension = DateDimension.buildDate(timestamp, DateTypeEnum.DAY);


        //platform维度,用于封装outputKey中的platFormDimension
        String platFormName = dimensions[7];
        platFormDimension = new PlatFormDimension();
        platFormDimension.setPlatFormName(platFormName);



        //*************************对browser_new_user指标进行封装*********************
        kpiDimension = new KpiDimension();
        kpiDimension.setKpiName(KpiTypeEnum.BROWSE_NEW_USER.getKpiType());
        statsCommonDimension = new StatsCommonDimension(dateDimension, platFormDimension,kpiDimension);
        //浏览器维度
        String browseName = dimensions[9];
        String browseVersion = dimensions[10];
        if(!StringUtils.isEmpty(browseName)&&!StringUtils.isEmpty(browseVersion)){
            browseDimension = new BrowseDimension(browseName, browseVersion);
            outputKey = new StatsUserDimension(statsCommonDimension, browseDimension);
            context.write(outputKey,outputValue);
        }


        //***********************定义仅有new_user指标时候的封装****************************
        kpiDimension = new KpiDimension();
        kpiDimension.setKpiName(KpiTypeEnum.NEW_USER.getKpiType());
        statsCommonDimension = new StatsCommonDimension(dateDimension, platFormDimension, kpiDimension);
        outputKey = new StatsUserDimension(statsCommonDimension, new BrowseDimension("", ""));
        context.write(outputKey, outputValue);

        outputRecord++;
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.warn("inputRecords:"+inputRecord+"\n"+"filterRecords:"+filterRecord+"\n"+
                "outputRecord:"+outputRecord);
    }












        //*************************我的*******************

    //进行mysql关联的实例
//    private IDimensionInfo iDimensionInfo;

    //    @Override
    //    protected void setup(Context context) throws IOException, InterruptedException {
    ////        iDimensionInfo= new DimensionInfoImpl();
    //    }

    //        //时间维度
    //        long timestamp = Long.valueOf(dimensions[1]);
    //        dateDimension = DateDimension.buildDate(timestamp, DateTypeEnum.DAY);
    ////        int dateDimensionId = iDimensionInfo.getDimensionIdByDim(dateDimension);
    ////        dateDimension.setId(dateDimensionId);

    //       //platform维度
    //        String platFormName = dimensions[7];
    //        platFormDimension = new PlatFormDimension();
    //        platFormDimension.setPlatFormName(platFormName);
    ////        int platFormDimensionId = iDimensionInfo.getDimensionIdByDim(platFormDimension);
    ////        platFormDimension.setId(platFormDimensionId);

    //        //浏览器维度
    //        String browseName = dimensions[9];
    //        String browseVersion = dimensions[10];
    //        browseDimension = new BrowseDimension(browseName, browseVersion);
    ////        int browseDimensionId = iDimensionInfo.getDimensionIdByDim(browseDimension);
    ////        browseDimension.setId(browseDimensionId);




//        //封装stats_device_browser  新增用户指标
//        String kpiNameBrowserNewUser = KpiTypeEnum.BROWSE_NEW_USER.getKpiType();
//        kpiDimension = new KpiDimension();
//        kpiDimension.setKpiName(kpiNameBrowserNewUser);
//        int browserNewUserKpiDimensioId = iDimensionInfo.getDimensionIdByDim(kpiDimension);
//        kpiDimension.setId(browserNewUserKpiDimensioId);
//        statsCommonDimension = new StatsCommonDimension(dateDimension,platFormDimension,kpiDimension);
//        statsUserDimension = new StatsUserDimension(statsCommonDimension, browseDimension);
//        context.write(statsUserDimension,mapOutPutWritable);
//
//
//
//
//        //封装stats_user 新增用户指标
//        String kpiNameNewUser = KpiTypeEnum.NEW_USER.getKpiType();
//        kpiDimension = new KpiDimension();
//        kpiDimension.setKpiName(kpiNameNewUser);
//        int newUserKpiId = iDimensionInfo.getDimensionIdByDim(kpiDimension);
//        kpiDimension.setId(newUserKpiId);
//        browseDimension = new BrowseDimension(0,"","");
//        statsCommonDimension = new StatsCommonDimension(dateDimension, platFormDimension,kpiDimension);
//        statsUserDimension = new StatsUserDimension(statsCommonDimension,browseDimension);
//        context.write(statsUserDimension,mapOutPutWritable);



}
