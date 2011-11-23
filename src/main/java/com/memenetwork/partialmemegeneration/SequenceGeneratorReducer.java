package com.memenetwork.partialmemegeneration;
import java.io.IOException;

import com.memenetwork.common.io.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SequenceGeneratorReducer
    extends Reducer <BucketPiece, Bucket, Text, SequenceArrayWritable> {
    private String currDocID;
    private SequenceBuilder seqB;
    private Text docIDWrapper;
    @Override public void setup(Context context)
        throws IOException, InterruptedException {
        super.setup(context);
        currDocID = null;
        seqB = null;
        docIDWrapper = new Text();
    }
    @Override public void reduce(BucketPiece key, Iterable <Bucket> values, Context context) throws IOException, InterruptedException {
        String newDocID = key.getDocID().toString();
        int currPosition = key.getPosition().get();
        if ((currDocID == null)||(! currDocID.equals(newDocID)) ) {
            if (seqB != null) {
                SequenceArrayWritable out = seqB.getSequenceArrayWritable();
                docIDWrapper.set(currDocID);
                context.write(docIDWrapper, out);
            }
            currDocID = newDocID;
            seqB = new SequenceBuilder(currDocID);
        }
        for (Bucket value: values) {
            seqB.build(value, currPosition);
        }
    }
    @Override public void cleanup (Context context)
        throws IOException, InterruptedException {
        if (seqB != null) {
            SequenceArrayWritable out = seqB.getSequenceArrayWritable();
            docIDWrapper.set(currDocID);
            context.write(docIDWrapper, out);
        }
    }
}
