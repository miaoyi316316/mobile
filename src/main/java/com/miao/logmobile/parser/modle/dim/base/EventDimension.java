package com.miao.logmobile.parser.modle.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class EventDimension extends BaseDimension{

    private int id;
    private String category;
    private String action;

    public EventDimension(){
        super();
    }
    public EventDimension(String category, String action) {
        this.category = category;
        this.action = action;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this==o){
            return 0;
        }
        EventDimension other = (EventDimension) o;
        int temp = this.id - other.id;
        if(temp!=0){
            return temp;
        }else {
            temp = this.category.compareTo(other.category);

            if(temp!=0){
                return temp;
            }else {
                return this.action.compareTo(other.action);
            }
        }

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.category);
        out.writeUTF(this.action);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.id = in.readInt();
        this.category = in.readUTF();
        this.action = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventDimension that = (EventDimension) o;
        return id == that.id &&
                Objects.equals(category, that.category) &&
                Objects.equals(action, that.action);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, category, action);
    }
}
