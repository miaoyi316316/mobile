package com.miao.logmobile.parser.mr;

import com.miao.logmobile.common.KpiTypeEnum;
import com.miao.logmobile.parser.modle.dim.base.BrowseDimension;
import com.miao.logmobile.parser.modle.dim.base.KpiDimension;
import com.miao.logmobile.parser.modle.dim.keys.StatsUserDimension;
import com.miao.logmobile.parser.modle.dim.value.map.MapOutPutWritable;
import com.miao.logmobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class NewUserValueReducer extends Reducer<StatsUserDimension, MapOutPutWritable, StatsUserDimension, ReduceOutputWritable> {

    private ReduceOutputWritable reduceOutputWritable;
    private MapWritable mapWritable;
    private KpiDimension kpiDimension;
    private KpiTypeEnum kpiTypeEnum;

    private static final Logger logger = Logger.getLogger(NewUserValueReducer.class);
    private Set<String> distinctSet;
//
//    private Text mapWritableKey;
//    private IntWritable mapWritableValueInt ;

    @Override
    protected void reduce(StatsUserDimension key, Iterable<MapOutPutWritable> values, Context context) throws IOException, InterruptedException {
        //将相同维度的value信息中的uid进行去重（放入set）然后求个数（size）
        distinctSet = new HashSet<>();

        String uid = null;

        for(MapOutPutWritable value:values){
            uid = value.getId();
            distinctSet.add(uid);
        }

        mapWritable = new MapWritable();
        reduceOutputWritable = new ReduceOutputWritable();

        IntWritable new_install_users = new IntWritable(distinctSet.size());



        //当前kpi信息
        kpiDimension = key.getStatsCommmonDimension().getKpiDimension();

        if(KpiTypeEnum.NEW_USER.getKpiType().equals(kpiDimension.getKpiName())){

            mapWritable.put(new Text(KpiTypeEnum.NEW_USER.getKpiType()), new_install_users);

            reduceOutputWritable.setKpi(KpiTypeEnum.NEW_USER);
            reduceOutputWritable.setValue(mapWritable);

            context.write(key,reduceOutputWritable);

        }else if (KpiTypeEnum.BROWSE_NEW_USER.getKpiType().equals(kpiDimension.getKpiName())){

            mapWritable.put(new Text(KpiTypeEnum.BROWSE_NEW_USER.getKpiType()), new_install_users);

            reduceOutputWritable.setKpi(KpiTypeEnum.BROWSE_NEW_USER);
            reduceOutputWritable.setValue(mapWritable);

            context.write(key,reduceOutputWritable);


        }else {
            logger.warn("当前指标不在统计范畴");
        }



//        //实例化reduce输出端的 value
//        mapWritable = new MapWritable();
//
//
//        kpiDimension = key.getStatsCommmonDimension().getKpiDimension();
//
//        kpiTypeEnum = KpiTypeEnum.valueOfKpiType(kpiDimension.getKpiName());
//        //new user 指标
//        if (kpiTypeEnum.equals(KpiTypeEnum.NEW_USER)) {
//            System.out.println("reduce端匹配到new user"+key.getBrowseDimension().getId());
//            //date维度表的映射
//            mapWritableKey = new Text();
//            mapWritableValueInt = new IntWritable();
//            mapWritableKey.set("date_dimension_id");
//            mapWritableValueInt.set(key.getStatsCommmonDimension().getDateDimension().getId());
//            mapWritable.put(mapWritableKey,mapWritableValueInt);
//
//            //platform维度表的映射
//            mapWritableKey = new Text();
//            mapWritableValueInt = new IntWritable();
//            mapWritableKey.set("platform_dimension_id");
//            mapWritableValueInt.set(key.getStatsCommmonDimension().getPlatFormDimension().getId());
//            mapWritable.put(mapWritableKey, mapWritableValueInt);
//
//            //new_install_users字段的值
//            mapWritableKey = new Text();
//            mapWritableValueInt = new IntWritable();
//            mapWritableKey.set("new_install_users");
//            mapWritableValueInt.set(new_install_users);
//            mapWritable.put(mapWritableKey, mapWritableValueInt);
//
//            //封装成Reduce的输出Value
//            reduceOutputWritable = new ReduceOutputWritable();
//            reduceOutputWritable.setValue(mapWritable);
//            reduceOutputWritable.setKpi(kpiTypeEnum);
//
//            context.write(key,reduceOutputWritable);
//        }//browser new user指标
//        else if(kpiTypeEnum.equals(KpiTypeEnum.BROWSE_NEW_USER)){
//            System.out.println("reduce端匹配到browse"+key.getBrowseDimension().getId());
//            //date维度表的映射
//            mapWritableKey = new Text();
//            mapWritableValueInt = new IntWritable();
//            mapWritableKey.set("date_dimension_id");
//            mapWritableValueInt.set(key.getStatsCommmonDimension().getDateDimension().getId());
//            mapWritable.put(mapWritableKey, mapWritableValueInt);
//
//            //platform维度表的映射
//            mapWritableKey = new Text();
//            mapWritableValueInt = new IntWritable();
//            mapWritableKey.set("platform_dimension_id");
//            mapWritableValueInt.set(key.getStatsCommmonDimension().getPlatFormDimension().getId());
//            mapWritable.put(mapWritableKey, mapWritableValueInt);
//
//            //browser维度表的映射
//            mapWritableKey = new Text();
//            mapWritableValueInt = new IntWritable();
//            mapWritableKey.set("browser_dimension_id");
//            mapWritableValueInt.set(key.getBrowseDimension().getId());
//            mapWritable.put(mapWritableKey, mapWritableValueInt);
//
//            //new_install_user字段的值
//            mapWritableKey = new Text();
//            mapWritableValueInt = new IntWritable();
//            mapWritableKey.set("new_install_users");
//            mapWritableValueInt.set(new_install_users);
//            mapWritable.put(mapWritableKey, mapWritableValueInt);
//
//            //reduce输出Value
//            reduceOutputWritable = new ReduceOutputWritable();
//            reduceOutputWritable.setValue(mapWritable);
//            reduceOutputWritable.setKpi(kpiTypeEnum);
//            context.write(key,reduceOutputWritable);
//
//        }else {
//            throw new RuntimeException("在reduce端发现了无法匹配的指标类型");
//        }



    }
}
