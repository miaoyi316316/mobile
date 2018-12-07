package com.miao.logmobile.parser.modle.dim.keys;

import com.miao.logmobile.parser.modle.dim.StatsBaseDimension;
import com.miao.logmobile.parser.modle.dim.StatsCommonDimension;
import com.miao.logmobile.parser.modle.dim.base.BrowseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class StatsUserDimension extends StatsBaseDimension {

    private StatsCommonDimension statsCommmonDimension = new StatsCommonDimension();

    private BrowseDimension browseDimension = new BrowseDimension();

    public StatsUserDimension(){
        super();
    }
    public StatsUserDimension(StatsCommonDimension statsCommonDimension){
        this.statsCommmonDimension = statsCommonDimension;
    }

    public StatsUserDimension(StatsCommonDimension statsCommmonDimension, BrowseDimension browseDimension) {
        this(statsCommmonDimension);
        this.browseDimension = browseDimension;
    }


    public StatsCommonDimension getStatsCommmonDimension() {
        return statsCommmonDimension;
    }

    public void setStatsCommmonDimension(StatsCommonDimension statsCommmonDimension) {
        this.statsCommmonDimension = statsCommmonDimension;
    }

    public BrowseDimension getBrowseDimension() {
        return browseDimension;
    }

    public void setBrowseDimension(BrowseDimension browseDimension) {
        this.browseDimension = browseDimension;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsUserDimension that = (StatsUserDimension) o;
        return Objects.equals(statsCommmonDimension, that.statsCommmonDimension) &&
                Objects.equals(browseDimension, that.browseDimension);
    }

    @Override
    public int hashCode() {

        return Objects.hash(statsCommmonDimension, browseDimension);
    }

    @Override
    public int compareTo(StatsBaseDimension o) {

        if(this==o){
            return 0;
        }
        StatsUserDimension other = (StatsUserDimension) o;

        int temp = this.statsCommmonDimension.compareTo(other.statsCommmonDimension);

        return temp!=0?temp:this.browseDimension.compareTo(other.browseDimension);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        this.statsCommmonDimension.write(dataOutput);
        this.browseDimension.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.statsCommmonDimension.readFields(dataInput);
        this.browseDimension.readFields(dataInput);
    }
}
