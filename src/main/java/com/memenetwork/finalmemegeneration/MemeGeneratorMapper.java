package com.memenetwork.finalmemegeneration;
import com.memenetwork.common.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MemeGeneratorMapper extends
Mapper <Text, SequenceArrayWritable, Text, MemeArrayWritable> {
    private Sequence activeSequence;
    private MemeArrayWritable memeStore;
    private Meme currMeme;
    private int shingleSize;
    @Override public void setup(Context context)
        throws IOException, InterruptedException{
        memeStore = new MemeArrayWritable ();
        shingleSize = context.getConfiguration().getInt("shinglesize", 0);
    }
    @Override public void map(Text key, SequenceArrayWritable value, Context context)
        throws IOException, InterruptedException{
        int ii = 0;
        memeStore.clear();
        currMeme = new Meme();
        activeSequence = null;
        while (ii < value.size()) {
            Sequence currSequence = value.get(ii);
            if ((activeSequence == null) ||
                    (isLonger(activeSequence, currSequence)) ) {
                activeSequence = currSequence;
                currMeme.setIndex(activeSequence.getIndex());
                currMeme.setLength(
                        computeNumUnigrams(activeSequence.getLength()));
                currMeme.addDocID(activeSequence.getOtherID());
            }
            else if (checkOverlap(activeSequence, currSequence)) {
                currMeme.setLength(
                    currMeme.getLength() +
                    computeNumUnigrams(currSequence.getLength()));
                currMeme.addDocID(currSequence.getOtherID());
            }
            else {
                memeStore.add(currMeme);
                currMeme = new Meme();
                activeSequence = currSequence;
                currMeme.setIndex(activeSequence.getIndex());
                currMeme.setLength(
                        computeNumUnigrams(activeSequence.getLength()));
                currMeme.addDocID(activeSequence.getOtherID());
            }
            ii++;
        }
        context.write(key, memeStore);
    }
    private int computeNumUnigrams(int numOverlappingShingles) {
        return numOverlappingShingles + shingleSize - 1;
    }
    private boolean checkOverlap(Sequence a, Sequence b) {
        int aLastIndex = a.getIndex() + computeNumUnigrams(a.getLength()) - 1;
        if ( (b.getIndex() >= a.getIndex() ) && (b.getIndex() <= aLastIndex)) {
            return true;
        }
        return false;
    }
    private boolean isLonger(Sequence activeSequence, Sequence otherSequence) {
        int aIndex = activeSequence.getIndex();
        int oIndex = otherSequence.getIndex();
        int aLength = activeSequence.getLength();
        int oLength = otherSequence.getLength();
        if( (aIndex == oIndex) && (aLength < oLength) ){
            return true;
        }
        return false;
    }
}

