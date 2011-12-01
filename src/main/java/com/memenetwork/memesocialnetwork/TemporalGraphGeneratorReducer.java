package com.memenetwork.memesocialnetwork;
import java.io.IOException;
import java.util.Set;
import java.util.HashSet;

import com.memenetwork.common.io.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TemporalGraphGeneratorReducer
    extends Reducer <Text, Text, Text, Text> {
    private StringBuilder adjacencyListBuilder;
    private Text adjacencyList;
    private Set <String> uniqueNodes;
    @Override public void setup(Context context)
        throws IOException, InterruptedException {
        super.setup(context);
        adjacencyList = new Text();
        uniqueNodes = new HashSet <String> ();
    }
    @Override public void reduce(Text key, Iterable <Text> values, Context context)
        throws IOException, InterruptedException {
        adjacencyListBuilder = new StringBuilder();
        uniqueNodes.clear();
        for(Text value: values) {
            String currValue = value.toString();
            String [] nodes = currValue.split("\t");
            for(int ii =0 ; ii < nodes.length; ii++){
             uniqueNodes.add(nodes[ii]);
            }
        }
        for (String currNode : uniqueNodes) {
            adjacencyListBuilder.append(currNode.toString()).append("\t");
        }
        adjacencyList.set(adjacencyListBuilder.toString().trim());
        context.write(key, adjacencyList);
    }
}
