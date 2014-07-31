package cn.edu.sysu.distributedLSH.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;


public class CandidatePriorityQueue implements Writable {
    /**
     * For the candidate data point.
     * */
    private static class Candidate implements Comparable<Candidate> {
        int index;      // the index of the candidate
        double dist;    // the distance between the candidate and the query
        int[] point;    // the candidate data point


        /**
         * Constructor.
         * */
        Candidate(final int index, final double dist, final int dim,
                final int[] point) throws ArrayIndexOutOfBoundsException {
            this.index = index;
            this.dist = dist;
            this.point = new int[dim];
            for (int i = 0; i < dim; i++) {
                this.point[i] = point[i];
            }
        }
        
        /**
         * Copy constructor.
         * */
        Candidate(final Candidate other) {
            index = other.index;
            dist = other.dist;
            point = new int[other.point.length];
            for (int i = 0; i < point.length; i++) {
                point[i] = other.point[i];
            }
        }

        /**
         * set.
         * */
        public void set(final int index, final double dist, final int[] point)
                throws ArrayIndexOutOfBoundsException {
            this.index = index;
            this.dist = dist;
            for (int i = 0; i < point.length; i++) {
                this.point[i] = point[i];
            }
        }
        
        /**
         * set.
         * */
        public void set(final Candidate other) throws ArrayIndexOutOfBoundsException {
            index = other.index;
            dist = other.dist;
            for (int i = 0; i < point.length; i++) {
                point[i] = other.point[i];
            }
        }

        /**
         * Implement the method in the interface Comparable.
         * This is the compare function for the max-heap.
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


    private int dim = -1;
    private int queryID = -1;
    private int kNeighbors = -1;

    // The candQueue is a max-heap whose capacity is kNeighbors. The implementation of
    // max-heap is PriorityQueue. It can support finding top-k minimum value efficiently.
    private Queue<Candidate> candQueue = null;

    // We use an index set so that we can judge efficiently whether a data point has been checked.
    private Set<Integer> checkedIndexSet = null;


    /**
     * Constructor.
     * */
    public CandidatePriorityQueue() {}

    /**
     * Constructor.
     * */
    public CandidatePriorityQueue(final int dim, final int queryID, final int kNeighbors) {
        this.dim = dim;
        this.queryID = queryID;
        this.kNeighbors = kNeighbors;
        candQueue = new PriorityQueue<Candidate>(kNeighbors);
        checkedIndexSet = new HashSet<Integer>();
    }

    /**
     * size.
     * @return the size of candQueue
     * */
    public int size() {
        return candQueue.size();
    }

    /**
     * clear.
     * */
    public void clear() {
        if (candQueue != null) {
            candQueue.clear();
        }
        if (checkedIndexSet != null) {
            checkedIndexSet.clear();
        }
    }

    /**
     * hashCode.
     * */
    @Override
    public int hashCode() {
        return queryID;
    }

    /**
     * equals.
     * */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CandidatePriorityQueue) {
            CandidatePriorityQueue that = (CandidatePriorityQueue)obj;
            if (this.queryID == that.queryID) {
                return this.candQueue.equals(that.candQueue);
            }
        }
        return false;
    }

    /**
     * Given a collided point, update the candQueue.
     * @param index the index of the collided point
     * @param point the collided point
     * @param query the query
     * @param ratioRadius ratio * the current radius, which is cR
     * */
    public void update(final int index, final int[] point, final int[] query,
            final int ratioRadius) {
        if (checkedIndexSet.contains(index)) {
            // the collided point has been checked
            return;
        }

        // We will check the collided point.
        checkedIndexSet.add(index);
        // calculate the distance between the collided point and the query
        double dist = LSHTool.calcL2Distance(point, query, dim);

        // TODO Should this if statement be deleted?
        if (dist < ratioRadius) {
            if (candQueue.size() == kNeighbors) {
                // get the candidate with maximum dist in candQueue
                Candidate candidate = candQueue.peek();
                if (dist < candidate.dist) {
                    candQueue.poll();
                    // Update candidate to avoid new operation.
                    candidate.set(index, dist, point);
                    // insert the new candidate
                    candQueue.add(candidate);
                }
            } else {
                // insert the collided point to the candQueue
                candQueue.add(new Candidate(index, dist, dim, point));
            }
        }
    }
    
    public void merge(final CandidatePriorityQueue other) {
        List<Candidate> otherList = other.sortedList();
        
        for (Candidate cand : otherList) {
            if (checkedIndexSet.contains(cand.index)) {
                continue;
            }
            checkedIndexSet.add(cand.index);

            if (candQueue.size() == kNeighbors) {
                // get the candidate with maximum dist in candQueue
                Candidate maxCand = candQueue.peek();
                if (cand.dist < maxCand.dist) {
                    candQueue.poll();
                    // Update candidate to avoid new operation.
                    maxCand.set(cand);
                    // insert the new candidate
                    candQueue.add(maxCand);
                } else {
                    // since the otherList is sorted by dist in ascending order
                    break;
                }
            } else {
                // insert the cand to the candQueue
                candQueue.add(new Candidate(cand));
            }
        }
    }

    /**
     * sortedList.
     * @return the sorted list which is constructed from candQueue
     * */
    public List<Candidate> sortedList() {
        List<Candidate> list = new LinkedList<Candidate>(candQueue);

        // Sort the list by dist in ascending order.
        Collections.sort(list, new Comparator<Candidate>() {
            public int compare(Candidate a, Candidate b) {
                if (a.dist > b.dist) {
                    return 1;
                } else if (a.dist < b.dist) {
                    return -1;
                }
                return 0;
            }
        });

        return list;
    }

    /**
     * This method will convert the sorted list of the candQueue to a string.
     * */
    @Override
    public String toString() {
        List<Candidate> candList = sortedList();
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(queryID);
        stringBuilder.append(" ");
        stringBuilder.append(candList.size());
        stringBuilder.append("\n");

        for (Candidate candidate : candList) {
            stringBuilder.append(candidate.dist);
            stringBuilder.append(" ");
            stringBuilder.append(candidate.index);
            for (int i = 0; i < dim; i++) {
                stringBuilder.append(" ");
                stringBuilder.append(candidate.point[i]);
            }
            stringBuilder.append("\n");
        }
        stringBuilder.append("\n");

        return stringBuilder.toString();
    }

    /**
     * Implement the method in the interface Writable.
     * We do not need to serialize the checkedIndexSet.
     * @param out output stream
     * */
    public void write(final DataOutput out) throws IOException {
        out.writeInt(dim);
        out.writeInt(queryID);
        out.writeInt(kNeighbors);

        out.writeInt(candQueue.size());
        for (Candidate candidate : candQueue) {
            out.writeInt(candidate.index);
            out.writeDouble(candidate.dist);
            for (int i = 0; i < dim; i++) {
                out.writeInt(candidate.point[i]);
            }
        }
    }

    /**
     * Implement the method in the interface Writable.
     * @param out output stream
     * */
    public void readFields(final DataInput in) throws IOException {
        int index;
        double dist;

        this.clear();
        
        dim = in.readInt();
        queryID = in.readInt();
        kNeighbors = in.readInt();

        int[] point = new int[dim];

        if (candQueue == null) {
            candQueue = new PriorityQueue<Candidate>(kNeighbors);
        }
        if (checkedIndexSet == null) {
            checkedIndexSet = new HashSet<Integer>();
        }

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            index = in.readInt();
            // We need to reconstruct the checkedIndexSet.
            checkedIndexSet.add(index);
            dist = in.readDouble();
            for (int j = 0; j < dim; j++) {
                point[j] = in.readInt();
            }
            candQueue.add(new Candidate(index, dist, dim, point));
        }
    }

    /**
     * @param radiusID the radius ID
     * @param baseDir the base directory
     * @param conf
     * */
    public void saveToHdfs(final int radiusID, final String baseDir, final Configuration conf)
        throws IOException {
        FileSystem fs = FileSystem.get(conf);
        String fileName = baseDir + "/radius_" + radiusID + "/intermediate/" + queryID;

        Path outFile = new Path(fileName);
        if (fs.exists(outFile)) {
            LSHTool.printAndExit("Output file " + fileName + " already exists");
        }

        FSDataOutputStream out = fs.create(outFile);
        try {
            this.write(out);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            out.close();
        }
    }

    /**
     * @param radiusID the radius ID
     * @param baseDir the base directory
     * @param conf
     * */
    public void readFromHdfs(final int radiusID, final String baseDir,
            final Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        String fileName = baseDir + "/radius_" + radiusID + "/intermediate/" + queryID;

        Path inFile = new Path(fileName);
        if (!fs.exists(inFile)) {
            LSHTool.printAndExit("Input file " + fileName + " not found");
        }
        if (!fs.isFile(inFile)) {
            LSHTool.printAndExit("Input " + fileName + " should be a file");
        }

        FSDataInputStream in = fs.open(inFile);
        try {
            this.readFields(in);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            in.close();
        }
    }
}
