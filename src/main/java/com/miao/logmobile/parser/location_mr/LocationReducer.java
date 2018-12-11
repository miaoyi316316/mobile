package com.miao.logmobile.parser.location_mr;

import com.miao.logmobile.common.KpiTypeEnum;
import com.miao.logmobile.parser.active_mr.ActiveUserReducer;
import com.miao.logmobile.parser.modle.dim.base.BrowseDimension;
import com.miao.logmobile.parser.modle.dim.base.DateDimension;
import com.miao.logmobile.parser.modle.dim.base.PlatFormDimension;
import com.miao.logmobile.parser.modle.dim.keys.StatsLocationDimension;
import com.miao.logmobile.parser.modle.dim.value.map.MapOutPutWritable;
import com.miao.logmobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LocationReducer extends Reducer<StatsLocationDimension,MapOutPutWritable,StatsLocationDimension,ReduceOutputWritable> {


    private static final Logger logger = Logger.getLogger(LocationReducer.class);


    private ReduceOutputWritable outputValue;
    private MapWritable mapWritable;

    private KpiTypeEnum kpi;
    private PlatFormDimension platFormDimension;
    private DateDimension dateDimension;

    private Set<String> uniqueUid;

    private Map<String, Integer> uniqueSid;

    @Override
    protected void reduce(StatsLocationDimension key, Iterable<MapOutPutWritable> values, Context context) throws IOException, InterruptedException {


        uniqueUid = new HashSet<>();//用于去重uid

        uniqueSid = new HashMap<>();//用于去重sid 和统计出只出现过一次的sid 也就是只访问了一个pv

        int sessionOnlyOne = 0;

        for(MapOutPutWritable value:values){

            String[] uidAndSid = value.getId().split(":");
            uniqueUid.add(uidAndSid[0]);
            if (!uniqueSid.containsKey(uidAndSid[1])) {
                uniqueSid.put(uidAndSid[1], 1);
                sessionOnlyOne++;
            }else {
                int count = uniqueSid.get(uidAndSid[1])+1;
                uniqueSid.put(uidAndSid[1],count);
            }

        }


        IntWritable activeUser = new IntWritable(uniqueUid.size());
        IntWritable sessionCount = new IntWritable(uniqueSid.size());
        IntWritable sessionOnlyPV = new IntWritable(sessionOnlyOne);

        mapWritable = new MapWritable();
        mapWritable.put(new IntWritable(1), activeUser);
        mapWritable.put(new IntWritable(2), sessionCount);
        mapWritable.put(new IntWritable(3), sessionOnlyPV);

        outputValue = new ReduceOutputWritable();

        outputValue.setValue(mapWritable);

        KpiTypeEnum kpiTypeEnum = KpiTypeEnum.valueOfKpiType(key.getStatsCommonDimension().getKpiDimension().getKpiName());

        outputValue.setKpi(kpiTypeEnum);

        context.write(key,outputValue);


    }
}
