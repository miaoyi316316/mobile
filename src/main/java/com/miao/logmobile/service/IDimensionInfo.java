package com.miao.logmobile.service;

import com.miao.logmobile.parser.modle.dim.base.BaseDimension;

import java.util.Date;

public interface IDimensionInfo {

    int getDimensionIdByDim(BaseDimension dimension);

    boolean isOldMember(String mid);

    int deleteOldMember(String todayTime);
}
