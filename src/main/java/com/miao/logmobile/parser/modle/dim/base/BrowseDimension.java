package com.miao.logmobile.parser.modle.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class BrowseDimension extends BaseDimension{

    private int id;
    private String browseName;
    private String browseVersion;

    public BrowseDimension(){
        super();
    }

    public BrowseDimension(String browseName, String browseVersion) {
        this.browseName = browseName;
        this.browseVersion = browseVersion;
    }

    public BrowseDimension(int id, String browseName, String browseVersion) {
        this(browseName, browseVersion);
        this.id = id;

    }

    @Override
    public int compareTo(BaseDimension o) {

        if(this==o){
            return 0;
        }
        BrowseDimension other = (BrowseDimension) o;

        int temp = this.id - other.id;
        if(temp!=0){
            return temp;
        }else {
            temp = this.browseName.compareTo(other.browseName);
            if(temp!=0)
                return temp;
            else {
                temp = this.browseVersion.compareTo(other.browseVersion);
                return temp;
            }
        }

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(browseName);
        dataOutput.writeUTF(browseVersion);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.browseName = dataInput.readUTF();
        this.browseVersion = dataInput.readUTF();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrowseDimension that = (BrowseDimension) o;
        return id == that.id &&
                Objects.equals(browseName, that.browseName) &&
                Objects.equals(browseVersion, that.browseVersion);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getBrowseName() {
        return browseName;
    }

    public void setBrowseName(String browseName) {
        this.browseName = browseName;
    }

    public String getBrowseVersion() {
        return browseVersion;
    }

    public void setBrowseVersion(String browseVersion) {
        this.browseVersion = browseVersion;
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, browseName, browseVersion);
    }
}
