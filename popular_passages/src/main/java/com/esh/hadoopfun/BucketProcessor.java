package com.esh.hadoopfun;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;

public class BucketProcessor {
    private List <Pair <Integer, Integer>> bucket;
    private Set <Integer> docIDs;
    private Map <Integer, List <Integer> > bucketMap;
    private void populateDataStructures(String inputString) {
        String [] bucketPiecesStr = inputString.trim().split("\\s+");
        for (String bucketPieceStr: bucketPiecesStr) {
            String [] bucketStrs = bucketPieceStr.split(",");
            int docID = Integer.parseInt(bucketStrs[0]);
            int position = Integer.parseInt(bucketStrs[1]);
            Pair <Integer, Integer> bucketPiece = new Pair <Integer,
                 Integer>(docID, position);
            bucket.add(bucketPiece);
            docIDs.add(docID);
            List <Integer> positions;
            if(! bucketMap.containsKey(docID)) {
                positions = new ArrayList <Integer> ();
            }
            else {
                positions = bucketMap.get(docID);
            }
            positions.add(position);
            bucketMap.put(docID, positions);
        }
    }
    public BucketProcessor(String inputString) {
        bucket = new ArrayList <Pair <Integer, Integer>> ();
        docIDs = new HashSet <Integer> ();
        bucketMap = new HashMap <Integer, List <Integer> > ();
        populateDataStructures(inputString);
    }
    public List <Pair <Integer, Integer>> getPairs() {
        return this.bucket;
    }
    public Set <Integer> getDocIDs() {
        return docIDs;
    }
    public List <Integer> getPositions(int docID) {
        return bucketMap.get(docID);
    }
}

