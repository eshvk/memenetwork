package com.memenetwork.partialmemegeneration;
import java.io.IOException;
import java.io.StringReader;

import com.memenetwork.common.io.BucketPiece;
import com.memenetwork.common.io.Bucket;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class ShingleTableMapper
    extends Mapper <LongWritable, Text, Text, BucketPiece> {
    private BucketPiece bucketPiece;
    private Text shingle;
    private int shingleSize;
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
        StringReader reader = new StringReader(tokens[2]);
        ShingleFilter stream = new ShingleFilter(
                new StandardTokenizer(Version.LUCENE_35, reader),
                                            shingleSize, shingleSize);
        stream.setOutputUnigrams(false);
        CharTermAttribute termAttribute = stream.getAttribute(
                                            CharTermAttribute.class);
        int ii = 0;
        while (stream.incrementToken()!= false) {
            bucketPiece.set(docID, ii);
            System.out.println("Shingle: " + termAttribute.toString() + "(" + ii + "," + docID + ")");
            ii++;
            shingle.set(termAttribute.toString());
            context.write(shingle, bucketPiece);
            termAttribute.setEmpty();
        }
    }
}

