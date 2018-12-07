package com.miao.logmobile.parser.modle.dim.value.map;

import com.miao.logmobile.common.KpiTypeEnum;

import com.miao.logmobile.parser.modle.dim.value.StatsBaseOutputDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MapOutPutWritable extends StatsBaseOutputDimension {


    private String id;//可以是uid/mid/sessionId等，通用

    private long sessionTime;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getSessionTime() {
        return sessionTime;
    }

    public void setSessionTime(long sessionTime) {
        this.sessionTime = sessionTime;
    }

    @Override
    public KpiTypeEnum getKpiTypeName() {
        return null;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(this.id);
        dataOutput.writeLong(sessionTime);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.sessionTime = dataInput.readLong();
    }
}
