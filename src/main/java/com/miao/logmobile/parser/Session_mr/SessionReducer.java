package com.miao.logmobile.parser.Session_mr;

import com.miao.logmobile.common.KpiTypeEnum;
import com.miao.logmobile.parser.active_mr.ActiveUserReducer;
import com.miao.logmobile.parser.modle.dim.base.BrowseDimension;
import com.miao.logmobile.parser.modle.dim.base.DateDimension;
import com.miao.logmobile.parser.modle.dim.base.PlatFormDimension;
import com.miao.logmobile.parser.modle.dim.keys.StatsUserDimension;
import com.miao.logmobile.parser.modle.dim.value.map.MapOutPutWritable;
import com.miao.logmobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class SessionReducer extends Reducer<StatsUserDimension,MapOutPutWritable,StatsUserDimension,ReduceOutputWritable> {

    private static final Logger logger = Logger.getLogger(SessionReducer.class);


    private ReduceOutputWritable outputValue;
    private MapWritable mapWritable;

    private BrowseDimension browseDimension;
    private KpiTypeEnum kpi;
    private PlatFormDimension platFormDimension;
    private DateDimension dateDimension;


    private Map<String,TreeSet<Long>> sessionsInfo ;


    int count = 0;
    @Override
    protected void reduce(StatsUserDimension key, Iterable<MapOutPutWritable> values, Context context) throws IOException, InterruptedException {

        count++;
        sessionsInfo = new HashMap<>();

        for(MapOutPutWritable value:values) {

            String sessionID = value.getId();

            long sessionTime = value.getSessionTime();
            if(!sessionsInfo.containsKey(sessionID)){
                TreeSet<Long> timeInfo = new TreeSet<>();
                timeInfo.add(sessionTime);
                sessionsInfo.put(sessionID, timeInfo);
            }else if(sessionsInfo.containsKey(sessionID)){
               sessionsInfo.get(sessionID).add(sessionTime);
            }
        }

        long totalTime = 0;
        int sessionSize = 0;
        for(Map.Entry<String,TreeSet<Long>> entry:sessionsInfo.entrySet()){

            long thisTime = 0;
            if (entry.getValue().size() == 1) {
                continue;
            }else {
                thisTime = (entry.getValue().last() - entry.getValue().first()) / 1000+1;
                sessionSize++;
            }
            totalTime += thisTime;
        }


        IntWritable sessionCount = new IntWritable(sessionSize);

        IntWritable sessionTotalTime = new IntWritable((int)totalTime);

        kpi = KpiTypeEnum.valueOfKpiType(key.getStatsCommmonDimension().getKpiDimension().getKpiName());


        mapWritable = new MapWritable();

        outputValue = new ReduceOutputWritable();


        //向reduce的输出mapWritable中封装session个数和各个session持续时间的总和
        mapWritable.put(new IntWritable(-1), sessionCount);
        mapWritable.put(new IntWritable(0), sessionTotalTime);

        outputValue.setValue(mapWritable);
        outputValue.setKpi(kpi);
        context.write(key,outputValue);



    }

}

