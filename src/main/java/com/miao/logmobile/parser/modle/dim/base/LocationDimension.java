package com.miao.logmobile.parser.modle.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class LocationDimension extends BaseDimension {

    private int id;
    private String country;
    private String province;
    private String city;


    public LocationDimension(){
        super();
    }

    public LocationDimension(String country, String province, String city) {
        this.country = country;
        this.province = province;
        this.city = city;
    }

    public LocationDimension(int id, String country, String province, String city) {
        this(country, province, city);
        this.id = id;

    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocationDimension that = (LocationDimension) o;
        return id == that.id &&
                Objects.equals(country, that.country) &&
                Objects.equals(province, that.province) &&
                Objects.equals(city, that.city);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, country, province, city);
    }

    @Override
    public int compareTo(BaseDimension o) {

        if(this==o){
            return 0;
        }
        LocationDimension other = (LocationDimension) o;
        int temp = this.id - other.id;
        if(temp!=0){
            return temp;
        }else {
            temp = this.country.compareTo(other.country);
            if(temp!=0){
                return temp;
            }else {
                temp = this.province.compareTo(other.province);
                if(temp!=0){
                    return temp;

                }else {
                    temp = this.city.compareTo(other.city);
                    return temp;
                }
            }
        }

    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(this.id);
        out.writeUTF(this.country);
        out.writeUTF(this.province);
        out.writeUTF(this.city);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.id = in.readInt();
        this.country = in.readUTF();
        this.province = in.readUTF();
        this.city = in.readUTF();
    }
}
