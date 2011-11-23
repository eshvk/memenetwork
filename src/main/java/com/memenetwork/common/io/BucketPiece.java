package com.memenetwork.common.io;
import java.io.*;
import org.apache.hadoop.io.*;

public class BucketPiece implements WritableComparable<BucketPiece> {

  private Text docID;
  private IntWritable position;

  public BucketPiece() {
    set(new Text(), new IntWritable());
  }

  public BucketPiece(String docID, Integer position) {
    set(new Text(docID), new IntWritable(position));
  }

  public BucketPiece(Text docID, IntWritable position) {
    set(docID, position);
  }
  public void set(String docID, Integer position) {
      this.docID = new Text(docID);
      this.position = new IntWritable(position);
  }

  public void set(Text docID, IntWritable position) {
    this.docID = docID;
    this.position = position;
  }

  public Text getDocID() {
    return docID;
  }

  public IntWritable getPosition() {
    return position;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    docID.write(out);
    position.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    docID.readFields(in);
    position.readFields(in);
  }

  @Override
  public int hashCode() {
    return docID.hashCode() * 163 + position.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof BucketPiece) {
      BucketPiece tp = (BucketPiece) o;
      return docID.equals(tp.docID) && position.equals(tp.position);
    }
    return false;
  }

  @Override
  public String toString() {
    return docID.toString() + "," + position.toString();
  }

  @Override
  public int compareTo(BucketPiece tp) {
    int cmp = docID.compareTo(tp.docID);
    if (cmp != 0) {
      return cmp;
    }
    return position.compareTo(tp.position);
  }
}
