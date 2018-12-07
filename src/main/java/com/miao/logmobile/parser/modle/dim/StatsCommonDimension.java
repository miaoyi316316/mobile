package com.miao.logmobile.parser.modle.dim;

import com.miao.logmobile.parser.modle.dim.base.DateDimension;
import com.miao.logmobile.parser.modle.dim.base.KpiDimension;
import com.miao.logmobile.parser.modle.dim.base.PlatFormDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class StatsCommonDimension extends StatsBaseDimension {


    private DateDimension dateDimension = new DateDimension();

    private PlatFormDimension platFormDimension = new PlatFormDimension();

    private KpiDimension kpiDimension = new KpiDimension();


    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    public PlatFormDimension getPlatFormDimension() {
        return platFormDimension;
    }

    public void setPlatFormDimension(PlatFormDimension platFormDimension) {
        this.platFormDimension = platFormDimension;
    }

    public KpiDimension getKpiDimension() {
        return kpiDimension;
    }

    public void setKpiDimension(KpiDimension kpiDimension) {
        this.kpiDimension = kpiDimension;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsCommonDimension that = (StatsCommonDimension) o;
        return Objects.equals(dateDimension, that.dateDimension) &&
                Objects.equals(platFormDimension, that.platFormDimension) &&
                Objects.equals(kpiDimension, that.kpiDimension);
    }

    @Override
    public int hashCode() {

        return Objects.hash(dateDimension, platFormDimension, kpiDimension);
    }

    public StatsCommonDimension(){
        super();
    }

    public StatsCommonDimension(DateDimension dateDimension, PlatFormDimension platFormDimension) {
        this.dateDimension = dateDimension;
        this.platFormDimension = platFormDimension;
    }

    public StatsCommonDimension(DateDimension dateDimension, PlatFormDimension platFormDimension, KpiDimension kpiDimension) {
        this(dateDimension, platFormDimension);
        this.kpiDimension = kpiDimension;
    }

    @Override
    public int compareTo(StatsBaseDimension o) {

        if (this==o){
            return 0;
        }
        StatsCommonDimension other = (StatsCommonDimension) o;
        int temp = this.dateDimension.compareTo(other.dateDimension);
        if(temp!=0){
            return temp;
        }else {
            temp = this.platFormDimension.compareTo(other.platFormDimension);
            if(temp!=0){
                return temp;
            }else {
                temp = this.kpiDimension.compareTo(other.kpiDimension);
                return temp;
            }
        }

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {


        this.dateDimension.write(dataOutput);
        this.platFormDimension.write(dataOutput);
        this.kpiDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {


        this.dateDimension.readFields(dataInput);
        this.platFormDimension.readFields(dataInput);
        this.kpiDimension.readFields(dataInput);

    }
}
