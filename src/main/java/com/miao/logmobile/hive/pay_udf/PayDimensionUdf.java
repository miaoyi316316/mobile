package com.miao.logmobile.hive.pay_udf;

import com.miao.logmobile.parser.modle.dim.base.PaymentDimension;
import com.miao.logmobile.service.DimensionInfoImpl;
import com.miao.logmobile.service.IDimensionInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class PayDimensionUdf extends UDF {

    public int evaluate(String pay){
        if(StringUtils.isEmpty(pay)){
            pay = "unknown";
        }
        IDimensionInfo iDimensionInfo = new DimensionInfoImpl();
        PaymentDimension paymentDimension = new PaymentDimension(pay);

        return iDimensionInfo.getDimensionIdByDim(paymentDimension);
    }
}
