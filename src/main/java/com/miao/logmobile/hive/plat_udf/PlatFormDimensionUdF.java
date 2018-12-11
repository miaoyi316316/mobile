package com.miao.logmobile.hive.plat_udf;

import com.miao.logmobile.parser.modle.dim.base.PlatFormDimension;
import com.miao.logmobile.service.DimensionInfoImpl;
import com.miao.logmobile.service.IDimensionInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class PlatFormDimensionUdF extends UDF {

    public int evaluate(String platName){

        if(StringUtils.isEmpty(platName)){
            platName = "unknown";
        }

        IDimensionInfo iDimensionInfo = new DimensionInfoImpl();

        PlatFormDimension platFormDimension = new PlatFormDimension(platName);

        return iDimensionInfo.getDimensionIdByDim(platFormDimension);


    }
}
