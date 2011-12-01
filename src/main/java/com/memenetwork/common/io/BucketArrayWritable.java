package com.memenetwork.common.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class BucketArrayWritable implements Writable {
    private List <Bucket> array;

    public BucketArrayWritable() {
        array = new ArrayList <Bucket> ();
    }
    public void add(Bucket currBucket) {
        Bucket copyBucket = new Bucket();
        for (int ii = 0; ii < currBucket.size();ii ++ ) {
            copyBucket.add(currBucket.get(ii));
        }
        array.add(copyBucket);
    }
    public Bucket get(int index) {
        return array.get(index);
    }
    public int size() {
        return array.size();
    }
    public void readFields(DataInput in) throws IOException {
        int nToRead = in.readInt();
        array.clear();
        while (nToRead > 0 ) {
            Bucket currB = new Bucket();
            currB.readFields(in);
            array.add(currB);
            nToRead--;
        }
    }
    public void clear() {
        array.clear();
    }
    public void write(DataOutput out) throws IOException {
        out.writeInt(array.size());
        for (Bucket s : array) {
            s.write(out);
        }
    }
    public String toString() {
        return array.toString();
    }
}
