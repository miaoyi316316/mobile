package com.miao.logmobile.parser.modle.dim.value.reduce;


import com.miao.logmobile.common.KpiTypeEnum;
import com.miao.logmobile.parser.modle.dim.value.StatsBaseOutputDimension;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReduceOutputWritable extends StatsBaseOutputDimension {

    private MapWritable value = new MapWritable();

    private KpiTypeEnum kpi;


    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }

    public void setKpi(KpiTypeEnum kpi) {
        this.kpi = kpi;
    }

    @Override
    public KpiTypeEnum getKpiTypeName() {

        return kpi;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        this.value.write(dataOutput);
        WritableUtils.writeEnum(dataOutput,kpi);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.value.readFields(dataInput);
        WritableUtils.readEnum(dataInput, KpiTypeEnum.class);
    }

}
