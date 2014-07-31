package cn.edu.sysu.distributedLSH.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class IntTriple implements WritableComparable<IntTriple> {
    private int first;
    private int second;
    private int third;


    /**
     * Constructor.
     * */
    public IntTriple() {
        set(-1, -1, -1);
    }

    /**
     * Constructor.
     * @param first the first integer
     * @param second the second integer
     * @param third the third integer
     * */
    public IntTriple(final int first, final int second, final int third) {
        set(first, second, third);
    }

    /**
     * set.
     * @param first the first integer
     * @param second the second integer
     * @param third the third integer
     * */
    public void set(final int first, final int second, final int third) {
        this.first = first;
        this.second = second;
        this.third = third;
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
     * get the third integer.
     * @return the third integer.
     * */
    public int getThird() {
        return third;
    }

    /**
     * hashCode.
     * */
    @Override
    public int hashCode() {
        return first * 79 + second * 31 + third;
    }

    /**
     * equals.
     * */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IntTriple) {
            IntTriple that = (IntTriple)obj;
            if (this.first == that.first && this.second == that.second
                    && this.third == that.third) {
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
        return first + " " + second + " " + third;
    }

    /**
     * write.
     * @param out output stream
     * @throws IOException
     * */
    public void write(final DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
        out.writeInt(third);
    }

    /**
     * readFields.
     * @param in input stream
     * @throws IOException
     * */
    public void readFields(final DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
        third = in.readInt();
    }

    /**
     * readFields.
     * @param that another IntTriple to be compared
     * @return
     * 0:  this = that
     * 1:  this > that
     * -1: this < that
     * */
    public int compareTo(final IntTriple that) {
        int firstResult = LSHTool.compareInts(this.first, that.first);
        
        if (firstResult != 0) {
            return firstResult;
        } else {
            int secondResult = LSHTool.compareInts(this.second, that.second);
            return secondResult == 0 ? LSHTool.compareInts(this.third, that.third) : secondResult;
        }
    }

    /**
     * A raw comparator optimized for IntTriple.
     * */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(IntTriple.class);
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
                int secondResult = LSHTool.compareInts(thisSecond, thatSecond);
                
                if (0 == secondResult) {
                    int thisThird = readInt(b1, s1 + 8);
                    int thatThird = readInt(b2, s2 + 8);
                    return LSHTool.compareInts(thisThird, thatThird);
                } else {
                    return secondResult;
                }
            } else {
                return firstResult;
            }
        }
    }

    static {
        // register this comparator
        WritableComparator.define(IntTriple.class, new Comparator());
    }
}
