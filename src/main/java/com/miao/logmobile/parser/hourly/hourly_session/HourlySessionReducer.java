package com.miao.logmobile.parser.hourly.hourly_session;

import com.miao.logmobile.common.KpiTypeEnum;
import com.miao.logmobile.parser.Session_mr.SessionReducer;
import com.miao.logmobile.parser.modle.dim.base.BrowseDimension;
import com.miao.logmobile.parser.modle.dim.base.DateDimension;
import com.miao.logmobile.parser.modle.dim.base.PlatFormDimension;
import com.miao.logmobile.parser.modle.dim.keys.StatsUserDimension;
import com.miao.logmobile.parser.modle.dim.value.map.MapOutPutWritable;
import com.miao.logmobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
import com.miao.logmobile.timeTransform.TimeTransform;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class HourlySessionReducer extends Reducer<StatsUserDimension,MapOutPutWritable,StatsUserDimension,ReduceOutputWritable> {

    private static final Logger logger = Logger.getLogger(HourlySessionReducer.class);


    private ReduceOutputWritable outputValue;
    private MapWritable mapWritable;

    private BrowseDimension browseDimension;
    private KpiTypeEnum kpi;
    private PlatFormDimension platFormDimension;
    private DateDimension dateDimension;


    private Map<Integer,Map<String,TreeSet<Long>>> sessionsInfo ;

    private Map<String,TreeSet<Long>> innerMap;

    private Map<Integer,Set<String>> hourSessions;

    private void setMap(MapWritable map){

        for(int i =0;i<24;i++){
            map.put(new IntWritable(i), new IntWritable(0));
        }
    }
    @Override
    protected void reduce(StatsUserDimension key, Iterable<MapOutPutWritable> values, Context context) throws IOException, InterruptedException {


        mapWritable = new MapWritable();
        setMap(mapWritable);

        kpi = KpiTypeEnum.valueOfKpiType(key.getStatsCommmonDimension().getKpiDimension().getKpiName());

        if (KpiTypeEnum.HOURLY_SESSIONS.equals(kpi)){

            hourSessions = new HashMap<>();

            for(MapOutPutWritable value:values){

                String sid = value.getId();
                int hour = TimeTransform.getHourlyByTime(value.getSessionTime());
                if(!hourSessions.containsKey(hour)){
                    Set<String> sessions = new HashSet<>();
                    sessions.add(sid);
                    hourSessions.put(hour, sessions);
                }else {
                    hourSessions.get(hour).add(sid);
                }

            }

            for(Map.Entry<Integer,Set<String>> entry:hourSessions.entrySet()){

                mapWritable.put(new IntWritable(entry.getKey()),new IntWritable(entry.getValue().size()));
            }

        }else if (KpiTypeEnum.HOURLY_SESSIONS_LENGTH.equals(kpi)){

            //sessionsInfo <小时，Map<sid,TreeSet<time>>>
            sessionsInfo = new HashMap<>();

            for(MapOutPutWritable value:values) {

                String sessionID = value.getId();

                long sessionTime = value.getSessionTime();

                int hour = TimeTransform.getHourlyByTime(sessionTime);
                if(!sessionsInfo.containsKey(hour)){
                    Map<String, TreeSet<Long>> oneInfo = new HashMap<>();
                    TreeSet<Long> timeInfo = new TreeSet<>();
                    timeInfo.add(sessionTime);
                    oneInfo.put(sessionID, timeInfo);
                    sessionsInfo.put(hour,oneInfo);
                }else {
                    innerMap = sessionsInfo.get(hour);
                    if(!innerMap.containsKey(sessionID)){
                        TreeSet<Long> timeInfo = new TreeSet<>();
                        timeInfo.add(sessionTime);
                        innerMap.put(sessionID, timeInfo);
                    }else if(innerMap.containsKey(sessionID)){
                        innerMap.get(sessionID).add(sessionTime);
                    }
                }

            }
            for(Map.Entry<Integer,Map<String,TreeSet<Long>>> mapEntry:sessionsInfo.entrySet()){
                //将每个小时的  多个session持续时长算出来
                int hourTotalTime = 0;
                for(Map.Entry<String,TreeSet<Long>> entry:mapEntry.getValue().entrySet()){
                    //下面是每个小时的每个sid  TreSet(time1,time2)
                    long thisTime = 0;
                    if (entry.getValue().size() == 1) {
                        continue;
                    }else {
                        thisTime = (entry.getValue().last() - entry.getValue().first()) / 1000+1;
                    }
                    hourTotalTime += thisTime;
                }
                //然后将每个小时的对应持续时长放到mapwritable
                mapWritable.put(new IntWritable(mapEntry.getKey()), new IntWritable(hourTotalTime));
            }


        }else {
            throw new RuntimeException("该kpi类型不匹配");
        }


        outputValue = new ReduceOutputWritable();

        outputValue.setValue(mapWritable);
        outputValue.setKpi(kpi);
        context.write(key,outputValue);



    }

}
