package com.memenetwork.common.io;
import java.io.*;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.*;


public class Meme implements Writable {

  private List<String> docIDs;
  private int startIndex;
  private int length;
  public Meme() {
      docIDs = new ArrayList<String> ();
      length = 0;
      startIndex = 0;
  }
  public void setLength(int length) {
      this.length = length;
  }
  public void setIndex(int startIndex) {
      this.startIndex = startIndex;
  }
  public void addDocID(String docID) {
      docIDs.add(docID);
  }
  public int getIndex() {
      return startIndex;
  }
  public String getDocID(int index) {
      return docIDs.get(index);
  }
  public int getNumDocIDs() {
      return docIDs.size();
  }
  public int getLength() {
      return length;
  }
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(startIndex);
    out.writeInt(length);
    out.writeInt(docIDs.size());
    int ii = 0;
    while(ii < docIDs.size()) {
        Text currDoc = new Text(docIDs.get(ii));
        currDoc.write(out);
        ii++ ;
    }
 }
  @Override
  public void readFields(DataInput in) throws IOException {
      startIndex = in.readInt();
      length = in.readInt();
      int size = in.readInt();
      while(size > 0) {
          Text currDoc = new Text();
          currDoc.readFields(in);
          docIDs.add(currDoc.toString());
          size--;
      }
  }
}
