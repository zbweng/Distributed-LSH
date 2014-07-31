package cn.edu.sysu.distributedLSH.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


/**
 * For the candidate data point.
 * */
public class Candidate implements WritableComparable<Candidate> {
    private int dim = -1;
    private double dist = -1;       // the distance between the candidate and the query
    private int[] point = null;     // the candidate data point


    /**
     * Default constructor.
     * */
    public Candidate() {}

    /**
     * Constructor.
     * */
    public Candidate(final int dim, final double dist, final int[] point)
            throws ArrayIndexOutOfBoundsException {
        this.dim = dim;
        this.dist = dist;
        this.point = new int[dim];
        for (int i = 0; i < dim; i++) {
            this.point[i] = point[i];
        }
    }
    
    /**
     * Copy constructor.
     * */
    public Candidate(final Candidate other) {
        this.dim = other.dim;
        this.dist = other.dist;
        this.point = new int[dim];
        for (int i = 0; i < dim; i++) {
            this.point[i] = other.point[i];
        }
    }
    
    /**
     * get dist.
     * */
    public double getDist() {
        return dist;
    }

    /**
     * Deep copy the Candidate.
     * @param other
     * */
    public void deepCopy(final Candidate other) throws ArrayIndexOutOfBoundsException {
        this.dim = other.dim;
        this.dist = other.dist;
        for (int i = 0; i < dim; i++) {
            this.point[i] = other.point[i];
        }
    }
    
    /**
     * Shallow set the Candidate. It only copy the reference of point.
     * */
    public void shallowSet(final double dist, final int[] point) {
        this.dim = point.length;
        this.dist = dist;
        this.point = point;
    }
    
    /**
     * toString.
     * */
    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(dist);
        for (int val : point) {
            stringBuilder.append(" ");
            stringBuilder.append(val);
        }
        stringBuilder.append("\n");

        return stringBuilder.toString();
    }
    
    /**
     * Implement the method in the interface Writable.
     * @param out output stream
     * */
    public void write(final DataOutput out) throws IOException {
        out.writeInt(dim);
        out.writeDouble(dist);
        for (int val : point) {
            out.writeInt(val);
        }
    }

    /**
     * Implement the method in the interface Writable.
     * @param out output stream
     * */
    public void readFields(final DataInput in) throws IOException {
        dim = in.readInt();
        dist = in.readDouble();
        // Since Writable is always reused by Hadoop, we can make use of
        // the current Candidate instance to avoid allocating memory.
        if (null == point) {
            point = new int[dim];
        }
        for (int i = 0; i < dim; i++) {
            point[i] = in.readInt();
        }
    }
    
    /**
     * Implement the method in the interface Comparable.
     * This is the compare function for PriorityQueue.
     * */
    public int compareTo(Candidate other) {
        if (this.dist < other.dist) {
            return 1;
        } else if (this.dist > other.dist) {
            return -1;
        }
        return 0;
    }
}
