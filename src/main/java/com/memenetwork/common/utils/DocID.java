package com.memenetwork.common.utils;
public class DocID implements Comparable<DocID> {
    private String completeString;
    private int year;
    private int month;
    private int day;
    private DocID() {
    }
    public DocID(String input) {
        completeString = input;
        String [] splits = input.split("-");
        String date = splits[1];
        year = Integer.parseInt(date.substring(0,4));
        month = Integer.parseInt(date.substring(4,6));
        day = Integer.parseInt(date.substring(6,8));
    }
    public String toString() {
        return completeString;
    }
    public int hashCode(){
        return completeString.hashCode();
    }
    public boolean equals(Object o) {
        if(o instanceof DocID) {
            DocID otherDoc = (DocID) o;
            return completeString.equals(otherDoc.completeString);
        }
        return false;
    }
    public int compareTo(DocID o) {
        if (!this.equals(o)) {
            if ((year == o.year) && (month == o.month) && (day == o.day)) {
                return 0;
            }
            else if (year < o.year) {
                return -1;
            }
            else if (year > o.year) {
                return 1;
            }
            else if (month < o.month) {
                return -1;
            }
            else if (month > o.month) {
                return 1;
            }
            else if (day < o.day) {
                return -1;
            }
            else if (day > o.day) {
                return 1;
            }
        }
        return 0;
    }
}
