package com.miao.logmobile.parser.location_mr;

import com.miao.logmobile.parser.modle.dim.keys.StatsLocationDimension;
import com.miao.logmobile.parser.modle.dim.value.map.MapOutPutWritable;
import com.miao.logmobile.parser.modle.dim.value.reduce.ReduceOutputWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class LocationReducer extends Reducer<StatsLocationDimension,MapOutPutWritable,StatsLocationDimension,ReduceOutputWritable> {


}
