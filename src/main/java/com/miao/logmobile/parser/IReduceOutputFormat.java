package com.miao.logmobile.parser;

import com.miao.logmobile.common.KpiTypeEnum;
import com.miao.logmobile.parser.modle.dim.StatsBaseDimension;
import com.miao.logmobile.parser.modle.dim.value.StatsBaseOutputDimension;
import com.miao.logmobile.service.IDimensionInfo;
import org.apache.hadoop.conf.*;

import java.sql.Connection;
import java.sql.PreparedStatement;

public interface IReduceOutputFormat {

    void buildInsertPs(IDimensionInfo iDimensionInfo, StatsBaseDimension key, StatsBaseOutputDimension value, PreparedStatement ps);



    String buildInsertSql(KpiTypeEnum kpiTypeEnum, Configuration conf);


}
