package cn.edu.sysu.distributedLSH.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class IntPair implements WritableComparable<IntPair> {
    private int first;
    private int second;


    /**
     * Constructor.
     * */
    public IntPair() {
        set(-1, -1);
    }

    /**
     * Constructor.
     * @param first the first integer
     * @param second the second integer
     * */
    public IntPair(final int first, final int second) {
        set(first, second);
    }

    /**
     * set.
     * @param first the first integer
     * @param second the second integer
     * */
    public void set(final int first, final int second) {
        this.first = first;
        this.second = second;
    }

    /**
     * get the first integer.
     * @return the first integer.
     * */
    public int getFirst() {
        return first;
    }

    /**
     * get the second integer.
     * @return the second integer.
     * */
    public int getSecond() {
        return second;
    }

    /**
     * hashCode.
     * */
    @Override
    public int hashCode() {
        return first * 31 + second;
    }

    /**
     * equals.
     * */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IntPair) {
            IntPair that = (IntPair)obj;
            if (this.first == that.first && this.second == that.second) {
                return true;
            }
        }
        return false;
    }

    /**
     * toString.
     * */
    @Override
    public String toString() {
        return first + " " + second;
    }

    /**
     * write.
     * @param out output stream
     * @throws IOException
     * */
    public void write(final DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    /**
     * readFields.
     * @param in input stream
     * @throws IOException
     * */
    public void readFields(final DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }

    /**
     * readFields.
     * @param that another IntPair to be compared
     * @return
     * 0:  this = that
     * 1:  this > that
     * -1: this < that
     * */
    public int compareTo(final IntPair that) {
        int firstResult = LSHTool.compareInts(this.first, that.first);
        return firstResult == 0 ?
            LSHTool.compareInts(this.second, that.second) : firstResult;
    }

    /**
     * A raw comparator optimized for IntPair.
     * */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(IntPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                byte[] b2, int s2, int l2) {
            int thisFirst = readInt(b1, s1);
            int thatFirst = readInt(b2, s2);
            int firstResult = LSHTool.compareInts(thisFirst, thatFirst);

            if (0 == firstResult) {
                int thisSecond = readInt(b1, s1 + 4);
                int thatSecond = readInt(b2, s2 + 4);
                return LSHTool.compareInts(thisSecond, thatSecond);
            } else {
                return firstResult;
            }
        }
    }

    static {
        // register this comparator
        WritableComparator.define(IntPair.class, new Comparator());
    }
}
