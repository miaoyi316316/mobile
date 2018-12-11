package com.miao.logmobile.hive.kpi_udf;

import com.miao.logmobile.parser.modle.dim.base.KpiDimension;
import com.miao.logmobile.service.DimensionInfoImpl;
import com.miao.logmobile.service.IDimensionInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class KpiDimensionUdf  extends UDF {


    public int evaluate(String kpiName){

        if(StringUtils.isEmpty(kpiName)){
            kpiName = "unknown";
        }

        IDimensionInfo iDimensionInfo = new DimensionInfoImpl();

        KpiDimension kpiDimension = new KpiDimension(kpiName);

        return iDimensionInfo.getDimensionIdByDim(kpiDimension);

    }
}
