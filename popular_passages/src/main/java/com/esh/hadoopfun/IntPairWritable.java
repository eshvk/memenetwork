package com.esh.hadoopfun;
// cc IntPairWritable A Writable implementation that stores a pair of IntWritable objects
// cc IntPairWritableComparator A RawComparator for comparing IntPairWritable byte representations
// cc IntPairWritableFirstComparator A custom RawComparator for comparing the first field of IntPairWritable byte representations
// vv IntPairWritable
// SOURCE HADOOP BOOK
import java.io.*;

import org.apache.hadoop.io.*;

public class IntPairWritable implements WritableComparable<IntPairWritable> {

  private IntWritable first;
  private IntWritable second;

  public IntPairWritable() {
    set(new IntWritable(), new IntWritable());
  }

  public IntPairWritable(Integer first, Integer second) {
    set(new IntWritable(first), new IntWritable(second));
  }

  public IntPairWritable(IntWritable first, IntWritable second) {
    set(first, second);
  }
  public void set(Integer first, Integer second) {
      this.first = new IntWritable(first);
      this.second = new IntWritable(second);
  }

  public void set(IntWritable first, IntWritable second) {
    this.first = first;
    this.second = second;
  }

  public IntWritable getFirst() {
    return first;
  }

  public IntWritable getSecond() {
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
    if (o instanceof IntPairWritable) {
      IntPairWritable tp = (IntPairWritable) o;
      return first.equals(tp.first) && second.equals(tp.second);
    }
    return false;
  }

  @Override
  public String toString() {
    return first.toString() + "," + second.toString();
  }

  @Override
  public int compareTo(IntPairWritable tp) {
    int cmp = first.compareTo(tp.first);
    if (cmp != 0) {
      return cmp;
    }
    return second.compareTo(tp.second);
  }
  // ^^ IntPairWritable
/*
  // vv IntPairWritableComparator
  public static class Comparator extends WritableComparator {

    private static final IntWritable.Comparator INT_COMPARATOR = new IntWritable.Comparator();

    public Comparator() {
      super(IntPairWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {

      try {
        int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
        int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
        int cmp = INT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
        if (cmp != 0) {
          return cmp;
        }
        return INT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                                       b2, s2 + firstL2, l2 - firstL2);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  static {
    WritableComparator.define(IntPairWritable.class, new Comparator());
  }
  // ^^ IntPairWritableComparator

  // vv IntPairWritableFirstComparator
  public static class FirstComparator extends WritableComparator {

    private static final IntWritable.Comparator INT_COMPARATOR = new IntWritable.Comparator();

    public FirstComparator() {
      super(IntPairWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {

      try {
        int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
        int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
        return INT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      if (a instanceof IntPairWritable && b instanceof IntPairWritable) {
        return ((IntPairWritable) a).first.compareTo(((IntPairWritable) b).first);
      }
      return super.compare(a, b);
    }
  }
  // ^^ IntPairWritableFirstComparator

// vv IntPairWritable
*/
}
// ^^ IntPairWritable
