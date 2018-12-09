package com.miao.logmobile.parser.pv_mr;

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

public class PvReducer extends Reducer<StatsUserDimension,MapOutPutWritable,StatsUserDimension,ReduceOutputWritable> {


    private static final Logger logger = Logger.getLogger(PvReducer.class);


    private ReduceOutputWritable outputValue;
    private MapWritable mapWritable;

    private BrowseDimension browseDimension;
    private KpiTypeEnum kpi;
    private PlatFormDimension platFormDimension;
    private DateDimension dateDimension;


    @Override
    protected void reduce(StatsUserDimension key, Iterable<MapOutPutWritable> values, Context context) throws IOException, InterruptedException {


        int pvs = 0;
        for(MapOutPutWritable value:values){

            pvs++;

        }

        IntWritable pvCount = new IntWritable(pvs);

        kpi = KpiTypeEnum.valueOfKpiType(key.getStatsCommmonDimension().getKpiDimension().getKpiName());

        mapWritable = new MapWritable();

        outputValue = new ReduceOutputWritable();

        mapWritable.put(new IntWritable(-1), pvCount);
        outputValue.setValue(mapWritable);
        outputValue.setKpi(kpi);
        context.write(key,outputValue);




    }

}
