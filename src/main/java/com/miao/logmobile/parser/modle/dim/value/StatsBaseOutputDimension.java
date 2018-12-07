package com.miao.logmobile.parser.modle.dim.value;

import com.miao.logmobile.common.KpiTypeEnum;

import org.apache.hadoop.io.Writable;

public abstract class StatsBaseOutputDimension implements Writable {

    public abstract KpiTypeEnum getKpiTypeName();


}
