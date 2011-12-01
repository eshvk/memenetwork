package com.memenetwork.partialmemegeneration;
import com.memenetwork.common.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SequenceGeneratorMapper extends
Mapper <Text, Bucket, BucketPiece, Bucket> {
        @Override public void map(Text key, Bucket value, Context context)
        throws IOException, InterruptedException{
        int size = value.size();
        for (int ii = 0; ii < size; ii ++ ) {
            context.write(value.get(ii), value);
        }
    }
}

