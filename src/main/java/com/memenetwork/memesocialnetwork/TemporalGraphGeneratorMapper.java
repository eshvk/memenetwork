package com.memenetwork.memesocialnetwork;
import com.memenetwork.common.io.*;
import com.memenetwork.common.utils.DocID;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Queue;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.HashSet;

public class TemporalGraphGeneratorMapper extends
Mapper <Text, MemeArrayWritable, Text, Text> {
    private Queue <DocID> sortedIDs;
    private Set <DocID> uniqueIDs;
    private Text currKey;
    private Text  currValue;
    @Override public void setup(Context context)
        throws IOException, InterruptedException{
        uniqueIDs = new HashSet <DocID> ();
        sortedIDs = new PriorityQueue <DocID> ();
        currKey = new Text();
        currValue = new Text();
    }
   private String constructString(Queue <DocID> components) {
        StringBuilder outString = new StringBuilder();
        for (DocID component: components) {
            outString.append(component.toString()).append("\t");
        }
        return outString.toString().trim();
    }
    @Override public void map(Text key, MemeArrayWritable value, Context context)
        throws IOException, InterruptedException{
        sortedIDs.clear();
        uniqueIDs.clear();
        int numMemes = value.size();
        uniqueIDs.add(new DocID(key.toString()));
        for (int ii = 0; ii < numMemes; ii ++ ) {
            Meme currMeme = value.get(ii);
            for (int jj = 0; jj < currMeme.getNumDocIDs(); jj++) {
                uniqueIDs.add(new DocID(currMeme.getDocID(jj)));
            }
        }
        sortedIDs.addAll(uniqueIDs);
        if (sortedIDs.size() > 0) {
            currKey.set(sortedIDs.poll().toString());
            String adjList = constructString(sortedIDs);
            currValue.set(adjList);
            context.write(currKey, currValue);
        }
    }
}

