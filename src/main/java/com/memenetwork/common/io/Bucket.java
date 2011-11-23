package com.memenetwork.common.io;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import org.apache.hadoop.io.Writable;

public class Bucket implements Writable {
    private List <BucketPiece> bucketPieces;
    private Set <BucketPiece> finder;

    public Bucket() {
        bucketPieces = new ArrayList <BucketPiece> ();
        finder = new HashSet <BucketPiece> ();
    }
    public void add(BucketPiece currBucketPiece) {
        BucketPiece bucketPiece = new BucketPiece();
        String currDocID = currBucketPiece.getDocID().toString();
        int currPosition = currBucketPiece.getPosition().get();
        bucketPiece.set(currDocID, currPosition);
        bucketPieces.add(bucketPiece);
        finder.add(bucketPiece);
    }
    public void clear() {
        bucketPieces.clear();
        finder.clear();
    }
    public BucketPiece get(int index) {
        return bucketPieces.get(index);
    }
    public int size() {
        return bucketPieces.size();
    }
    public boolean contains(BucketPiece b) {
        return finder.contains(b);
    }
    public void readFields(DataInput in) throws IOException {
        int nToRead = in.readInt();
        bucketPieces.clear();
        finder.clear();
        while (nToRead > 0 ) {
            BucketPiece bucketPiece = new BucketPiece();
            bucketPiece.readFields(in);
            bucketPieces.add(bucketPiece);
            finder.add(bucketPiece);
            nToRead--;
        }
    }
    public void write(DataOutput out) throws IOException {
        out.writeInt(bucketPieces.size());
        for (BucketPiece bp : bucketPieces) {
            bp.write(out);
        }
    }
}
