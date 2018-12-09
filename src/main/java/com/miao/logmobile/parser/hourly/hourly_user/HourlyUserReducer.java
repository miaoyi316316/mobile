package com.miao.logmobile.parser.hourly.hourly_user;

import com.miao.logmobile.common.KpiTypeEnum;
import com.miao.logmobile.parser.active_mr.ActiveUserReducer;
import com.miao.logmobile.parser.modle.dim.base.BrowseDimension;
import com.miao.logmobile.parser.modle.dim.base.DateDimension;
import com.miao.logmobile.parser.modle.dim.base.PlatFormDimension;
import com.miao.logmobile.parser.modle.dim.keys.StatsUserDimension;
import com.miao.logmobile.parser.modle.dim.value.map.MapOutPutWritable;
import com.miao.logmobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
import com.miao.logmobile.timeTransform.TimeTransform;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class HourlyUserReducer extends Reducer<StatsUserDimension,MapOutPutWritable,StatsUserDimension,ReduceOutputWritable> {


    private static final Logger logger = Logger.getLogger(ActiveUserReducer.class);


    private ReduceOutputWritable outputValue;
    private MapWritable mapWritable;

    private BrowseDimension browseDimension;
    private KpiTypeEnum kpi;
    private PlatFormDimension platFormDimension;
    private DateDimension dateDimension;

    private Map<Integer,Set<String>> hourUser;


    private void setMap(MapWritable map){

        for(int i =0;i<24;i++){
            map.put(new IntWritable(i), new IntWritable(0));
        }
    }

    @Override
    protected void reduce(StatsUserDimension key, Iterable<MapOutPutWritable> values, Context context) throws IOException, InterruptedException {


        hourUser = new TreeMap<>();

        // plat date kpi uid  要按照小时分开 需要每个小时的活跃用户 用mapWritable封装起来


//        distinct = new HashSet<>();
        for(MapOutPutWritable value:values){

            long currentTime = value.getSessionTime();

            String uid = value.getId();
            int hour = TimeTransform.getHourlyByTime(currentTime);

            if(!hourUser.containsKey(hour)){
                Set<String> uids = new HashSet<>();
                uids.add(uid);
                hourUser.put(hour, uids);
            }else {
                hourUser.get(hour).add(uid);
            }
        }

        mapWritable = new MapWritable();
        setMap(mapWritable);

        for(Map.Entry<Integer,Set<String>> entry:hourUser.entrySet()){

            mapWritable.put(new IntWritable(entry.getKey()),new IntWritable(entry.getValue().size()));
        }


        kpi = KpiTypeEnum.valueOfKpiType(key.getStatsCommmonDimension().getKpiDimension().getKpiName());

        outputValue = new ReduceOutputWritable();
        outputValue.setValue(mapWritable);
        outputValue.setKpi(kpi);
        context.write(key,outputValue);




    }
}
