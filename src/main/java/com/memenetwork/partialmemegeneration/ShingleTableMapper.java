package com.memenetwork.partialmemegeneration;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import com.memenetwork.common.io.BucketPiece;
import com.memenetwork.common.io.Bucket;
import com.memenetwork.common.utils.Pair;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ShingleTableMapper
    extends Mapper <LongWritable, Text, Text, BucketPiece> {
    private BucketPiece bucketPiece;
    private Text shingle;
    private int shingleSize;
    private String cleanToken(String token){
        return token.toLowerCase().replaceAll("\\W+", "");
    }
    private String formShingle(String [] unigrams, int start, int length) {
        StringBuilder shingleBuilder = new StringBuilder();
        int ii = 0;
        while(ii < length) {
            shingleBuilder.append(unigrams[start + ii]).append(" ");
            ii ++;
        }
        return shingleBuilder.toString().trim();
    }
    private List <Pair <String, Integer>>
        generateShingles(String document, int shingleSize) {
        String [] tokens = document.trim().split("\\s+");
        int ii = 0;
        List <String> cleanedTokens = new ArrayList <String> ();
        while(ii < tokens.length) {
            String cleanedToken = cleanToken(tokens[ii]);
            if (cleanedToken.trim().length() > 0) {
                cleanedTokens.add(cleanedToken.trim());
            }
            ii++;
        }
        tokens = cleanedTokens.toArray(new String [cleanedTokens.size()]);
        List <Pair <String, Integer>> shingles = new ArrayList
            <Pair <String, Integer>> ();
        Pair <String, Integer> currPair;
        for (ii = 0; (ii  + shingleSize - 1)< tokens.length; ii ++) {
            String currNgram = formShingle(tokens, ii, shingleSize);
            currPair = new Pair <String, Integer> (currNgram, ii);
            shingles.add(currPair);
        }
        return shingles;
        }
    @Override public void setup(Context context)
        throws IOException, InterruptedException {
        super.setup(context);
        bucketPiece = new BucketPiece();
        shingleSize = context.getConfiguration().getInt("shinglesize", 0);
        shingle = new Text();
    }
    @Override public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String [] tokens = value.toString().split("\t", 3);
        String docID = tokens[0];
        List <Pair <String, Integer> > shingleData =
            generateShingles(tokens[2], shingleSize);
        for (Pair <String, Integer> sd : shingleData) {
            shingle.set(sd.getFirst().trim());
            bucketPiece.set(docID, sd.getSecond());
            context.write(shingle, bucketPiece);
        }
    }
}

