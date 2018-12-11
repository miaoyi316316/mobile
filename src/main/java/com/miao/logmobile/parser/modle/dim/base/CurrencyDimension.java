package com.miao.logmobile.parser.modle.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CurrencyDimension extends  BaseDimension{

    private int id;

    private String currency_name;

    public CurrencyDimension(){
        super();
    }
    public CurrencyDimension(String currency_name) {
        this.currency_name = currency_name;
    }

    public CurrencyDimension(int id, String currency_name) {
        this.id = id;
        this.currency_name = currency_name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCurrency_name() {
        return currency_name;
    }

    public void setCurrency_name(String currency_name) {
        this.currency_name = currency_name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CurrencyDimension that = (CurrencyDimension) o;
        return id == that.id &&
                Objects.equals(currency_name, that.currency_name);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, currency_name);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this==o){
            return 0;
        }
        CurrencyDimension other = (CurrencyDimension) o;
        int temp = this.id - other.id;
        if(temp!=0){
            return temp;
        }else {
            return this.currency_name.compareTo(other.currency_name);
        }

    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(this.id);
        out.writeUTF(this.currency_name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.id = in.readInt();
        this.currency_name = in.readUTF();
    }
}
