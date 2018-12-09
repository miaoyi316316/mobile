package com.miao.logmobile.parser.modle.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class KpiDimension extends BaseDimension {

    private int id;
    private String kpiName;


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getKpiName() {
        return kpiName;
    }

    public void setKpiName(String kpiName) {
        this.kpiName = kpiName;
    }

    public KpiDimension(){
        super();
    }


    public KpiDimension( String kpiName) {

        this.kpiName = kpiName;
    }


    @Override
    public int compareTo(BaseDimension o) {
        if(this==o){
            return 0;
        }

        KpiDimension other = (KpiDimension) o;
        int temp = this.id - other.id;
        if(temp!=0){
            return temp;
        }else {
            temp = this.kpiName.compareTo(other.kpiName);
            return temp;
        }

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(kpiName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.kpiName = dataInput.readUTF();

    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KpiDimension that = (KpiDimension) o;
        return id == that.id &&
                Objects.equals(kpiName, that.kpiName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, kpiName);
    }
}
