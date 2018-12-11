package com.miao.logmobile.hive.currency_udf;

import com.miao.logmobile.parser.modle.dim.base.CurrencyDimension;
import com.miao.logmobile.service.DimensionInfoImpl;
import com.miao.logmobile.service.IDimensionInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class CurrencyDimensionUdf extends UDF {


    public int evaluate(String currency){


        if(StringUtils.isEmpty(currency)){
            currency = "unknown";
        }
        IDimensionInfo iDimensionInfo = new DimensionInfoImpl();

        CurrencyDimension currencyDimension = new CurrencyDimension(currency);

        return iDimensionInfo.getDimensionIdByDim(currencyDimension);

    }


}
