package com.memenetwork.memesocialnetwork;
import java.io.IOException;

import com.memenetwork.common.io.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TemporalGraphGeneratorReducer
    extends Reducer <Text, Text, Text, Text> {
    private StringBuilder adjacencyListBuilder;
    private Text adjacencyList;
    @Override public void setup(Context context)
        throws IOException, InterruptedException {
        super.setup(context);
        adjacencyList = new Text();
    }
    @Override public void reduce(Text key, Iterable <Text> values, Context context)
        throws IOException, InterruptedException {
        adjacencyListBuilder = new StringBuilder();
        for(Text value: values) {
            adjacencyListBuilder.append(value.toString()).append("\t");
        }
        adjacencyList.set(adjacencyListBuilder.toString().trim());
        context.write(key, adjacencyList);
    }
    @Override public void cleanup (Context context)
        throws IOException, InterruptedException {
    }
}
