package com.memenetwork.common.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class SequenceArrayWritable implements Writable {
    private List <Sequence> array;

    public SequenceArrayWritable() {
        array = new ArrayList <Sequence> ();
    }
    public void add(Sequence currSequence) {
        String otherID = currSequence.getOtherID();
        int index = currSequence.getIndex();
        int otherIndex = currSequence.getOtherIndex();
        int length = currSequence.getLength();
        Sequence copySeq = new Sequence(index, length, otherID, otherIndex);
        array.add(copySeq);
    }
    public Sequence get(int index) {
        return array.get(index);
    }
    public int size() {
        return array.size();
    }
    public void readFields(DataInput in) throws IOException {
        int nToRead = in.readInt();
        array.clear();
        while (nToRead > 0 ) {
            Sequence currSeq = new Sequence();
            currSeq.readFields(in);
            array.add(currSeq);
            nToRead--;
        }
    }
    public void write(DataOutput out) throws IOException {
        out.writeInt(array.size());
        for (Sequence s : array) {
            s.write(out);
        }
    }
    public String toString() {
        return array.toString();
    }
}
