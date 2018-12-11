package com.miao.logmobile.hive.event_udf;

import com.miao.logmobile.parser.modle.dim.base.EventDimension;
import com.miao.logmobile.service.DimensionInfoImpl;
import com.miao.logmobile.service.IDimensionInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class EventDimensionUdF extends UDF {

    public int evaluate(String ca, String ac) {

        if(StringUtils.isEmpty(ca)){
            ca = "unknown";
        }
        if(StringUtils.isEmpty(ac)){
            ac = "unknown";
        }

        IDimensionInfo iDimensionInfo = new DimensionInfoImpl();
        EventDimension eventDimension = new EventDimension(ca,ac);

        return iDimensionInfo.getDimensionIdByDim(eventDimension);


    }
}
