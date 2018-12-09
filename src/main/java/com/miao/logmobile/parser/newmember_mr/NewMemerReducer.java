package com.miao.logmobile.parser.newmember_mr;

import com.miao.logmobile.common.KpiTypeEnum;
import com.miao.logmobile.parser.modle.dim.base.KpiDimension;
import com.miao.logmobile.parser.modle.dim.keys.StatsUserDimension;
import com.miao.logmobile.parser.modle.dim.value.map.MapOutPutWritable;
import com.miao.logmobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
import com.miao.logmobile.parser.newuser_mr.NewUserValueReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class NewMemerReducer extends Reducer<StatsUserDimension,MapOutPutWritable,StatsUserDimension,ReduceOutputWritable> {

    private ReduceOutputWritable reduceOutputWritable;
    private MapWritable mapWritable;
    private KpiDimension kpiDimension;
    private KpiTypeEnum kpiTypeEnum;

    private static final Logger logger = Logger.getLogger(NewMemerReducer.class);
    private Set<String> distinctSet;
    @Override
    protected void reduce(StatsUserDimension key, Iterable<MapOutPutWritable> values, Context context) throws IOException, InterruptedException {

        //将相同维度的value信息中的mid进行去重（放入set）然后求个数（size
        distinctSet = new HashSet<>();

        StringBuilder mids = new StringBuilder();


        for(MapOutPutWritable value:values){

            if(distinctSet.add(value.getId())){
                mids.append(value.getId()+"\t");
            }

        }
        mids.deleteCharAt(mids.length()-1);

        mapWritable = new MapWritable();
        reduceOutputWritable = new ReduceOutputWritable();

        IntWritable new_members = new IntWritable(distinctSet.size());


        //当前kpi信息
        kpiDimension = key.getStatsCommmonDimension().getKpiDimension();

        mapWritable.put(new IntWritable(-1), new_members);
        mapWritable.put(new IntWritable(0), new Text(mids.toString()));

        reduceOutputWritable.setKpi(KpiTypeEnum.valueOfKpiType(kpiDimension.getKpiName()));
        reduceOutputWritable.setValue(mapWritable);
        context.write(key,reduceOutputWritable);

    }
}
