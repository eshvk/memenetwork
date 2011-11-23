package com.memenetwork.partialmemegeneration;
import java.util.Set;
import java.util.TreeSet;
import com.memenetwork.common.io.*;
public class SequenceBuilder {
    private Set <Sequence> activeSequences;
    private Set <Sequence> sequences;
    private Set <Sequence> newAdditions;
    private int index;
    private String sourceID;
    private BucketPiece bp;
    public SequenceBuilder(String sourceID) {
        this.sourceID = sourceID;
        index = 0;
        sequences = new TreeSet <Sequence> ();
        activeSequences = new TreeSet <Sequence> ();
        newAdditions = new TreeSet <Sequence> ();
    }
    public void build(Bucket bucket, int currSourcePos) {
        newAdditions.clear();
        Set <BucketPiece> removals = new TreeSet <BucketPiece> ();
        if(currSourcePos != index) {
            terminateActiveSequences();
        }
        for (Sequence actS : activeSequences) {
            bp = new BucketPiece(actS.getOtherID(), actS.getOtherIndex() +
                    actS.getLength());
            if (bucket.contains(bp)) {
                actS.setLength(actS.getLength() + 1);
                removals.add(bp);
            }
            else {
                newAdditions.add(actS);
            }
        }
        for (Sequence s: newAdditions) {
            if (activeSequences.contains(s)) {
                activeSequences.remove(s);
            }
        }
        sequences.addAll(newAdditions);
        for (int ii = 0; ii < bucket.size(); ii ++ ) {
            BucketPiece currBP = bucket.get(ii);
            if (!(currBP.getDocID().toString().equals(sourceID))
                    && (!removals.contains(currBP))){
                Sequence actS = new Sequence(index, 1,
                        currBP.getDocID().toString(), currBP.getPosition().get());
                activeSequences.add(actS);
                    }
        }
        ++index;
    }
    private void terminateActiveSequences() {
        for (Sequence actS : activeSequences) {
            sequences.add(actS);
        }
        activeSequences.clear();
    }
    public SequenceArrayWritable getSequenceArrayWritable(){
        terminateActiveSequences();
        SequenceArrayWritable serializedOut = new SequenceArrayWritable();
        for (Sequence s: sequences) {
            serializedOut.add(s);
        }
        return serializedOut;
    }
}
