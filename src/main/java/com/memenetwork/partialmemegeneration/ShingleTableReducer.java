package com.memenetwork.partialmemegeneration;
import java.io.IOException;

import com.memenetwork.common.io.*;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public  class ShingleTableReducer
    extends Reducer <Text, BucketPiece, Text, Bucket> {
    private Bucket bucket;
    @Override public void setup(Context context)
        throws IOException, InterruptedException {
        super.setup(context);
        bucket = new Bucket();
    }
    @Override public void reduce(Text key, Iterable<BucketPiece> values, Context context)
        throws IOException, InterruptedException {
        bucket.clear();
        for (BucketPiece value : values) {
            bucket.add(value);
        }
        if (bucket.size() > 1) {
            context.write(key, bucket);
        }
    }
}

