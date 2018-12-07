package com.miao.logmobile.parser.modle.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PlatFormDimension extends BaseDimension {

    private int id;
    private String platFormName;


    public PlatFormDimension(){
        super();
    }
    public PlatFormDimension(int id, String platFormName) {
        this.id = id;
        this.platFormName = platFormName;

    }


    @Override
    public int compareTo(BaseDimension o) {

        if(this == o){
            return 0;
        }
        PlatFormDimension other = (PlatFormDimension) o;

        int temp = this.id - other.id;

        return temp != 0 ? temp : this.platFormName.compareTo(other.platFormName);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.platFormName);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.platFormName = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlatFormDimension that = (PlatFormDimension) o;
        return id == that.id &&
                Objects.equals(platFormName, that.platFormName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, platFormName);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPlatFormName() {
        return platFormName;
    }

    public void setPlatFormName(String platFormName) {
        this.platFormName = platFormName;
    }
}
