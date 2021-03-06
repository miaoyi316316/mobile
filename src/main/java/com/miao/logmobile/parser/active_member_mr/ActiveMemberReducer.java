package com.miao.logmobile.parser.active_member_mr;

import com.miao.logmobile.common.KpiTypeEnum;
import com.miao.logmobile.parser.active_mr.ActiveUserReducer;
import com.miao.logmobile.parser.modle.dim.base.BrowseDimension;
import com.miao.logmobile.parser.modle.dim.base.DateDimension;
import com.miao.logmobile.parser.modle.dim.base.PlatFormDimension;
import com.miao.logmobile.parser.modle.dim.keys.StatsUserDimension;
import com.miao.logmobile.parser.modle.dim.value.map.MapOutPutWritable;
import com.miao.logmobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ActiveMemberReducer extends Reducer<StatsUserDimension,MapOutPutWritable,StatsUserDimension,ReduceOutputWritable> {


    private static final Logger logger = Logger.getLogger(ActiveUserReducer.class);


    private ReduceOutputWritable outputValue;
    private MapWritable mapWritable;

    private BrowseDimension browseDimension;
    private KpiTypeEnum kpi;
    private PlatFormDimension platFormDimension;
    private DateDimension dateDimension;


    private Set<String> distinct;

    @Override
    protected void reduce(StatsUserDimension key, Iterable<MapOutPutWritable> values, Context context) throws IOException, InterruptedException {


        distinct = new HashSet<>();
        for(MapOutPutWritable value:values){

            distinct.add(value.getId());

        }

        IntWritable activeUsers = new IntWritable(distinct.size());

        kpi = KpiTypeEnum.valueOfKpiType(key.getStatsCommmonDimension().getKpiDimension().getKpiName());


        mapWritable = new MapWritable();
        outputValue = new ReduceOutputWritable();


        mapWritable.put(new IntWritable(-1), activeUsers);
        outputValue.setKpi(kpi);
        outputValue.setValue(mapWritable);
        context.write(key,outputValue);




    }
}
