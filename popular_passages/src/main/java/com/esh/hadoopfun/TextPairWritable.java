package com.esh.hadoopfun;
// cc TextPairWritable A Writable implementation that stores a pair of Text objects
// cc TextPairWritableComparator A RawComparator for comparing TextPairWritable byte representations
// cc TextPairWritableFirstComparator A custom RawComparator for comparing the first field of TextPairWritable byte representations
// vv TextPairWritable
// SOURCE HADOOP BOOK
import java.io.*;

import org.apache.hadoop.io.*;

public class TextPairWritable implements WritableComparable<TextPairWritable> {

  private Text first;
  private Text second;

  public TextPairWritable() {
    set(new Text(), new Text());
  }

  public TextPairWritable(String first, String second) {
    set(new Text(first), new Text(second));
  }

  public TextPairWritable(Text first, Text second) {
    set(first, second);
  }

  public void set(Text first, Text second) {
    this.first = first;
    this.second = second;
  }

  public Text getFirst() {
    return first;
  }

  public Text getSecond() {
    return second;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    first.write(out);
    second.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first.readFields(in);
    second.readFields(in);
  }

  @Override
  public int hashCode() {
    return first.hashCode() * 163 + second.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TextPairWritable) {
      TextPairWritable tp = (TextPairWritable) o;
      return first.equals(tp.first) && second.equals(tp.second);
    }
    return false;
  }

  @Override
  public String toString() {
    return first + "\t" + second;
  }

  @Override
  public int compareTo(TextPairWritable tp) {
    int cmp = first.compareTo(tp.first);
    if (cmp != 0) {
      return cmp;
    }
    if ( (second.toString().compareTo("ESHDICTASEKRIT")!= 0)
            && (tp.second.toString().compareTo("ESHDICTASEKRIT")!= 0) ) {
        return second.compareTo(tp.second);
    }
    else if ( (second.toString().compareTo("ESHDICTASEKRIT")== 0)
            && (tp.second.toString().compareTo("ESHDICTASEKRIT")== 0) ) {
        return 0;
    }
    else if ( (second.toString().compareTo("ESHDICTASEKRIT")== 0)
            && (tp.second.toString().compareTo("ESHDICTASEKRIT")!= 0) ) {
        return -1;
    }
    else{
        return 1;
    }
  }
  // ^^ TextPairWritable

  // vv TextPairWritableComparator
  public static class Comparator extends WritableComparator {

    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

    public Comparator() {
      super(TextPairWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {

      try {
        int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
        int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
        int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
        if (cmp != 0) {
          return cmp;
        }
        return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                                       b2, s2 + firstL2, l2 - firstL2);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  static {
    WritableComparator.define(TextPairWritable.class, new Comparator());
  }
  // ^^ TextPairWritableComparator

  // vv TextPairWritableFirstComparator
  public static class FirstComparator extends WritableComparator {

    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

    public FirstComparator() {
      super(TextPairWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {

      try {
        int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
        int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
        return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      if (a instanceof TextPairWritable && b instanceof TextPairWritable) {
        return ((TextPairWritable) a).first.compareTo(((TextPairWritable) b).first);
      }
      return super.compare(a, b);
    }
  }
  // ^^ TextPairWritableFirstComparator

// vv TextPairWritable
}
// ^^ TextPairWritable
