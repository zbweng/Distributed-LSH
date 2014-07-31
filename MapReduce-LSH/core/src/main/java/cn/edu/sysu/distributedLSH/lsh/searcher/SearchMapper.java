package cn.edu.sysu.distributedLSH.lsh.searcher;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.edu.sysu.distributedLSH.common.Candidate;
import cn.edu.sysu.distributedLSH.common.HashTable;
import cn.edu.sysu.distributedLSH.common.LSH;
import cn.edu.sysu.distributedLSH.common.LSHTool;
import cn.edu.sysu.distributedLSH.common.SimpleList;
import cn.edu.sysu.distributedLSH.common.TwoDArray;


public class SearchMapper extends Mapper<Object, Text, IntWritable, Candidate> {
    private static class CandidateNode implements Comparable<CandidateNode> {
        int index = -1;     // the index of the candidate
        double dist = -1;   // the distance between the candidate and the query
        
        /**
         * Constructor.
         * */
        CandidateNode(final int index, final double dist) {
            this.index = index;
            this.dist = dist;
        }
        
        /**
         * set.
         * */
        public void set(final int index, final double dist) {
            this.index = index;
            this.dist = dist;
        }
        
        /**
         * Implement the method in the interface Comparable.
         * This is the compare function for the max-heap.
         * */
        public int compareTo(CandidateNode other) {
            if (this.dist < other.dist) {
                return 1;
            } else if (this.dist > other.dist) {
                return -1;
            }
            return 0;
        }
    }
    

    private static class CandidateIndexHeap {
        int queryID = -1;
        int dim = -1;
        int kNeighbors = -1;

        int searchCount = 0;    // how many points have been searched for this query in a radius

        // The candQueue is a max-heap whose capacity is kNeighbors. The implementation of
        // max-heap is PriorityQueue. It can support finding top-k minimum value efficiently.
        Queue<CandidateNode> candQueue = null;

        // We use an index set so that we can judge efficiently whether a data point
        // has been checked.
        Set<Integer> checkedIndexSet = null;

        /**
         * Constructor.
         * */
        public CandidateIndexHeap(final int queryID, final int dim, final int kNeighbors) {
            this.queryID = queryID;
            this.dim = dim;
            this.kNeighbors = kNeighbors;
            candQueue = new PriorityQueue<CandidateNode>(kNeighbors);
            checkedIndexSet = new HashSet<Integer>();
        }
        
        /**
         * Given a collided point, update the candQueue.
         * @param index the index of the collided point in the part of data set
         * @param point the collided point
         * @param query the query
         * @param ratioRadius  equals to ratio * currentRadius, which is cR
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
            double curDist = LSHTool.calcL2Distance(point, query, dim);

            // TODO Should this if statement be deleted?
            if (curDist < ratioRadius) {
                if (candQueue.size() == kNeighbors) {
                    // get the candidate with maximum dist in candQueue
                    CandidateNode candNode = candQueue.peek();
                    if (curDist < candNode.dist) {
                        candQueue.poll();
                        // Update candidate to avoid allocating memory.
                        candNode.set(index, curDist);
                        // insert the new candidate
                        candQueue.add(candNode);
                    }
                } else {
                    // insert the collided point to the candQueue
                    candQueue.add(new CandidateNode(index, curDist));
                }
            }
        }
    }


    private static final int THRESHOLD_RADIUS = 1;
    
    private Configuration conf;
    private FileSystem fs;

    private int dimension;
    private int nRadii;
    private int querySetSize;
    private int ratio;
    private int kNeighbors;
    private int pruneFactor;
    private String baseDir;
    private String querySetFileName;

    private int partKNeighbors;
    private int[][] querySet = null;
    private int[] radii = null;
    private IntWritable queryIDWritable = new IntWritable();
    private Candidate candidate = new Candidate();


    /**
     * setup.
     * @param context
     * */
    @Override
    protected void setup(final Context context) {
        conf = context.getConfiguration();
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        dimension = conf.getInt("dimension", -1);
        nRadii = conf.getInt("nRadii", -1);
        querySetSize = conf.getInt("querySetSize", -1);
        kNeighbors = conf.getInt("kNeighbors", -1);
        ratio = conf.getInt("ratio", -1);

        pruneFactor = conf.getInt("pruneFactor", -1);
        if (pruneFactor < 1) {
            LSHTool.printAndExit("pruneFactor error");
        }

        baseDir = conf.get("baseDir");
        querySetFileName = conf.get("querySetFileName");
        
        // read query set first
        try {
            this.readQuerySet();
        } catch (IOException e) {
            e.printStackTrace();
            LSHTool.printAndExit("read query set failed");
        }
        
        // the number of neighbors that we should find in a partition
        partKNeighbors = this.calcPartKNeighbors();

        // initialize multiple radii
        radii = new int[nRadii];
        radii[0] = THRESHOLD_RADIUS;
        for (int i = 1; i < nRadii; i++) {
            radii[i] = ratio * radii[i - 1];
        }        
    }
    
    
    /**
     * Read query set from Hadoop's distributed cache.
     * */
    private void readQuerySet() throws IOException {
        if (0 == querySetSize || 0 == dimension) {
            LSHTool.printAndExit("querySetSize and dimension should not be zero");
        }

        File inputFile = new File(querySetFileName);
        Scanner scanner = null;
        try {
            scanner = new Scanner(inputFile, "UTF-8");

            // initialize querySet
            querySet = new int[querySetSize][];
            for (int i = 0; i < querySetSize; i++) {
                querySet[i] = new int[dimension];
            }

            for (int i = 0; i < querySetSize; i++) {
                // skip the query ID
                scanner.nextInt();
                for (int j = 0; j < dimension; j++) {
                    querySet[i][j] = scanner.nextInt();
                }
            }
        } finally {
            scanner.close();
        }
    }
    
    /**
     * Calculate the number of neighbors that we should find in a partition.
     * */
    private int calcPartKNeighbors() {
        return kNeighbors;
    }

    /**
     * map.
     * Search for queries using LSH.
     * @param key dummy
     * @param value contains the partition id
     * @param context
     * */
    @Override
    protected void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        String partDir = baseDir + "/part_" + parsePartID(value);

        TwoDArray partDataSet = new TwoDArray();
        // read the partition of the data set
        partDataSet.readFromHdfs(partDir, fs);
        int[][] points = partDataSet.get();     // points in the partition of the data set
        
        List<CandidateIndexHeap> queryList = new LinkedList<CandidateIndexHeap>();
        for (int i = 0; i < querySetSize; i++) {
            queryList.add(new CandidateIndexHeap(i, dimension, partKNeighbors));
        }
        
        LSH lsh = new LSH(dimension);
        // read LSH
        lsh.readFromHdfs(partDir, fs);
        // get some commonly used parameters
        int hashTableSize = lsh.getHashTableSize();

        // the maximum number of real distances to be calculated for a query
        int searchThreshold = pruneFactor * hashTableSize + partKNeighbors;

        for (int radiusID = 0; radiusID < nRadii; radiusID++) {
            // ratio * currentRadius
            int ratioRadius = ratio * radii[radiusID];

            for (int tableID = 0; tableID < hashTableSize; tableID++) {
                HashTable hashTable = new HashTable(radiusID, tableID);
                hashTable.readFromHdfs(partDir, fs);
                Map<Integer, SimpleList> hashTableMap = hashTable.get();

                Iterator<CandidateIndexHeap> it = queryList.iterator();
                while (it.hasNext()) {
                    CandidateIndexHeap candIndexHeap = it.next();
                    int bucketID = lsh.calcHashValue(tableID, radii[radiusID],
                            querySet[candIndexHeap.queryID]);
                    // get bucket in hash table by bucketID
                    SimpleList bucket = hashTableMap.get(bucketID);
                    if (null == bucket) {
                        continue;
                    }
                    if (this.collide(points, ratioRadius, searchThreshold, candIndexHeap, bucket)) {
                        // TODO delete
                        System.out.printf("Query: %d, radiusID: %d\n", candIndexHeap.queryID, radiusID);
                        
                        
                        // We have search for enough data points thus emit the search result.
                        this.emit(points, candIndexHeap, context);
                        // Remove the query from queryList.
                        it.remove();
                    }
                }
                if (queryList.isEmpty()) {
                    break;
                }
            }

            if (queryList.isEmpty()) {
                break;
            } else {
                // reset search count for the remaining queries
                for (CandidateIndexHeap candIndexHeap : queryList) {
                    candIndexHeap.searchCount = 0;
                }
            }
        }
    }
    
    /**
     * Parse the partition id.
     * @param value contains the partition id
     * */
    private int parsePartID(final Text value) {
        int partID = -1;
        Scanner scanner = null;
        try {
            scanner = new Scanner(value.toString());
            partID = scanner.nextInt();
        } finally {
            scanner.close();
        }
        return partID;
    }

    /**
     * Collide a query with a bucket. This is an auxiliary for map.
     * @param points contains the data points in the partition of the data set
     * @param ratioRadius that is ratio * currentRadius
     * @param searchThreshold the maximum number of real distances to be calculated for a query
     * @param candIndexHeap contains some staff of the query, such as query id,
     *  checked candidates, etc.
     * @param bucket the bucket which the query falls into
     * @return This method will return true if we have searched enough data points.
     * */
    private boolean collide(final int[][] points, final int ratioRadius, final int searchThreshold,
            final CandidateIndexHeap candIndexHeap, final SimpleList bucket) {
        bucket.setCursorToHead();
        while (bucket.hasNext()) {
            candIndexHeap.searchCount++;
            int candIndex = bucket.next();
            candIndexHeap.update(candIndex, points[candIndex], querySet[candIndexHeap.queryID],
                    ratioRadius);
            if (candIndexHeap.searchCount >= searchThreshold) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Emit the search result for a query. This is an auxiliary for map.
     * @param points contains the data points in the partition of the data set
     * @param candIndexHeap contains some staff of the query, such as query id,
     *  checked candidates, etc.
     * @param context
     * */
    private void emit(final int[][] points, final CandidateIndexHeap candIndexHeap,
            final Context context) throws IOException, InterruptedException {
        Queue<CandidateNode> candQueue = candIndexHeap.candQueue;
        queryIDWritable.set(candIndexHeap.queryID);

        while (!candQueue.isEmpty()) {
            CandidateNode candNode = candQueue.poll();
            // For the sake of efficiency, we shallow set the candidate to avoid memory allocation.
            candidate.shallowSet(candNode.dist, points[candNode.index]);
            context.write(queryIDWritable, candidate);
        }
    }
}
