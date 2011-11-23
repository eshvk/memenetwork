package com.memenetwork.common.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class MemeArrayWritable implements Writable {
    private List <Meme> array;

    public MemeArrayWritable() {
        array = new ArrayList <Meme> ();
    }
    public void add(Meme currMeme) {
        //TODO UNSAFE! You need to do a deep copy of currMeme
        array.add(currMeme);
    }
    public Meme get(int index) {
        return array.get(index);
    }
    public int size() {
        return array.size();
    }
    public void readFields(DataInput in) throws IOException {
        int nToRead = in.readInt();
        clear();
        while (nToRead > 0 ) {
            Meme currSeq = new Meme();
            currSeq.readFields(in);
            array.add(currSeq);
            nToRead--;
        }
    }
    public void write(DataOutput out) throws IOException {
        out.writeInt(array.size());
        for (Meme s : array) {
            s.write(out);
        }
    }
    public void clear(){
        array.clear();
    }
}
