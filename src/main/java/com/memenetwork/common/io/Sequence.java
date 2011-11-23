package com.memenetwork.common.io;
import java.io.*;
import org.apache.hadoop.io.*;

public class Sequence implements WritableComparable<Sequence> {
    private int index;
    private int length;
    private String otherID;
    private int otherIndex;
    public Sequence() {
        otherID = null;
    }
    public Sequence(int index, int length, String otherID, int otherIndex) {
        this.index = index;
        this.length = length;
        this.otherID = otherID;
        this.otherIndex = otherIndex;
    }
    @Override public String toString(){
        String index = Integer.toString(this.index);
        String length = Integer.toString(this.length);
        String otherIndex = Integer.toString(this.otherIndex);
        return index + ", " + length + " (" + otherID + ", " + otherIndex + ")";
    }
    public int getIndex() {
        return index;
    }
    public String getOtherID() {
        return otherID;
    }
    public int getOtherIndex() {
        return otherIndex;
    }
    public int getLength() {
        return length;
    }
    public void setLength(int length) {
        this.length = length;
    }
    @Override public int hashCode(){
        int hash = 1;
        hash = hash * 17 + index;
        hash = hash * 13 + length;
        hash = hash * 31 + otherID.hashCode();
        hash = hash * 11 + otherIndex;
        return hash;
    }
    @Override public boolean equals(Object obj){
        if(obj == this){
            return true;
        }
        if(obj == null || obj.getClass() != this.getClass()){
            return false;
        }
        Sequence other = (Sequence) obj;
        return (index == other.index) && (length == other.length) && (otherID.equals(other.otherID)) && (otherIndex == other.otherIndex);
    }
    @Override public void readFields(DataInput in) throws IOException {
        index = in.readInt();
        Text otherIDWrapper = new Text();
        otherIDWrapper.readFields(in);
        otherID = otherIDWrapper.toString();
        otherIndex = in.readInt();
        length = in.readInt();
    }
    @Override public void write(DataOutput out) throws IOException {
        out.writeInt(index);
        Text otherIDWrapper = new Text(otherID);
        otherIDWrapper.write(out);
        out.writeInt(otherIndex);
        out.writeInt(length);
    }
    @Override
        public int compareTo(Sequence tp) {
            int cmp;
            if (this.equals(tp)) {
                cmp = 0;
            }
            else if (index != tp.index) {
                cmp = (index < tp.index) ? -1 : 1;
            }
            else if (length != tp.length) {
                cmp = (length < tp.length) ? -1 : 1;
            }
            else if (!otherID.equals(tp.otherID)) {
                cmp = otherID.compareTo(tp.otherID);
            }
            else {
                cmp = (otherIndex < tp.otherIndex) ? -1 : 1;
            }
            return cmp;
        }

}

