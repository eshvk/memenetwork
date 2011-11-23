package com.memenetwork.partialmemegeneration;
import org.apache.hadoop.mapreduce.Partitioner;
import com.memenetwork.common.io.*;
public class SequenceGeneratorPartitioner extends  Partitioner <BucketPiece, Bucket> {
    @Override
        public int getPartition(BucketPiece key, Bucket value, int numPartitions) {
            return (key.getDocID().hashCode()  & Integer.MAX_VALUE) % numPartitions;
        }
}


