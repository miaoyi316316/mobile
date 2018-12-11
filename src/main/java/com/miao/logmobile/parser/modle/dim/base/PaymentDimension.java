package com.miao.logmobile.parser.modle.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PaymentDimension extends BaseDimension {

    private int id;
    private String payment_type;

    public PaymentDimension(){
        super();

    }

    public PaymentDimension(int id, String payment_type) {
        this.id = id;
        this.payment_type = payment_type;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPayment_type() {
        return payment_type;
    }

    public void setPayment_type(String payment_type) {
        this.payment_type = payment_type;
    }

    public PaymentDimension(String payment_type) {
        this.payment_type = payment_type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PaymentDimension that = (PaymentDimension) o;
        return id == that.id &&
                Objects.equals(payment_type, that.payment_type);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, payment_type);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this==o){
            return 0;
        }
        PaymentDimension other = (PaymentDimension) o;
        int temp = this.id - other.id;

        if(temp!=0){
            return temp;
        }else {
            return this.payment_type.compareTo(other.payment_type);
        }

    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(this.id);
        out.writeUTF(this.payment_type);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.id = in.readInt();
        this.payment_type = in.readUTF();

    }
}
