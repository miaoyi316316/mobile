package com.miao.logmobile.hive.date_udf;

import com.miao.logmobile.common.DateTypeEnum;
import com.miao.logmobile.parser.modle.dim.base.DateDimension;
import com.miao.logmobile.service.DimensionInfoImpl;
import com.miao.logmobile.service.IDimensionInfo;
import com.miao.logmobile.timeTransform.TimeTransform;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class DateDimensionUdf extends UDF {

    public int evaluate(String date){

        if(StringUtils.isEmpty(date)){

            return -1;
        }
        IDimensionInfo iDimensionInfo = new DimensionInfoImpl();
        long timestamp = TimeTransform.String2long(date, "yyyy-MM-dd");

        DateDimension dateDimension = DateDimension.buildDate(timestamp, DateTypeEnum.DAY);


        return iDimensionInfo.getDimensionIdByDim(dateDimension);
    }

}
