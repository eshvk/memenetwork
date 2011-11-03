package com.esh.hadoopfun;

public class Pair <U, V> {
    private U first;
    private V second;
    public  Pair(U first, V second){
        this.first = first;
        this.second = second;
    }
    public U getFirst(){
        return first;
    }
    public V getSecond(){
        return second;
    }
    @Override
        public final boolean equals(Object o) {
            if (!(o instanceof Pair<?,?>))
                return false;

            final Pair<?, ?> other = (Pair<?, ?>) o;
            return equal(getFirst(), other.getFirst()) && equal(getSecond(), other.getSecond());
        }
    public static final boolean equal(Object o1, Object o2) {
        if (o1 == null) {
            return o2 == null;
        }
        return o1.equals(o2);
    } // end method
    @Override
        public int hashCode() {
            int hLeft = getFirst() == null ? 0 : getFirst().hashCode();
            int hRight = getSecond() == null ? 0 : getSecond().hashCode();
            return hLeft + (37 * hRight);
        } // end method
}
