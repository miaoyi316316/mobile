package com.miao.logmobile.parser.modle.dim.keys;

import com.miao.logmobile.parser.modle.dim.StatsBaseDimension;
import com.miao.logmobile.parser.modle.dim.StatsCommonDimension;
import com.miao.logmobile.parser.modle.dim.base.LocationDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class StatsLocationDimension extends StatsBaseDimension {


    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();

    private LocationDimension locationDimension = new LocationDimension();


    public StatsLocationDimension(){
        super();
    }

    public StatsLocationDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public StatsLocationDimension(StatsCommonDimension statsCommonDimension, LocationDimension locationDimension) {
        this(statsCommonDimension);
        this.locationDimension = locationDimension;
    }


    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsLocationDimension that = (StatsLocationDimension) o;
        return Objects.equals(statsCommonDimension, that.statsCommonDimension) &&
                Objects.equals(locationDimension, that.locationDimension);
    }

    @Override
    public int hashCode() {

        return Objects.hash(statsCommonDimension, locationDimension);
    }

    public LocationDimension getLocationDimension() {
        return locationDimension;
    }

    public void setLocationDimension(LocationDimension locationDimension) {
        this.locationDimension = locationDimension;
    }

    @Override
    public int compareTo(StatsBaseDimension o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
}
