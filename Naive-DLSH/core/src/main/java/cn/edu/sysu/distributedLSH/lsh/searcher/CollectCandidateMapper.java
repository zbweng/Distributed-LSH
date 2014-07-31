package cn.edu.sysu.distributedLSH.lsh.searcher;

import static java.lang.Math.*;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.edu.sysu.distributedLSH.common.HashTableBlock;
import cn.edu.sysu.distributedLSH.common.LSHBlock;
import cn.edu.sysu.distributedLSH.common.LSHTool;
import cn.edu.sysu.distributedLSH.common.SimpleList;


public class CollectCandidateMapper extends Mapper<Object, Text, IntWritable, SimpleList> {
    private static final int THRESHOLD_RADIUS = 1;
    
    private Configuration conf;
    private FileSystem fs;

    private int ratio;
    private int dimension;
    private int querySetSize;
    private int radiusID;

    private String baseDir;
    private String querySetFileName;

    private int radius;
    private int[][] querySet = null;
    private boolean[] valid = null;

    private IntWritable queryIDWritable = new IntWritable();
    private SimpleList emptyList = new SimpleList();


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

        ratio = conf.getInt("ratio", 0);
        dimension = conf.getInt("dimension", 0);
        querySetSize = conf.getInt("querySetSize", 0);
        radiusID = conf.getInt("radiusID", -1);

        baseDir = conf.get("baseDir");
        querySetFileName = conf.get("querySetFileName");
        
        radius = (int)(THRESHOLD_RADIUS * pow(ratio, radiusID));
        
        try {
            this.readQuerySet();
        } catch (IOException e) {
            e.printStackTrace();
            LSHTool.printAndExit("read query set failed");
        }

        valid = new boolean[querySetSize];
        if (radiusID > 0) {
            this.readRemainingQueryID();
        } else {
            for (int i = 0; i < querySetSize; i++) {
                valid[i] = true;
            }
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
     * Read remaining query IDs from hdfs then initialize the valid array.
     * */
    private void readRemainingQueryID() {
        for (int i = 0; i < querySetSize; i++) {
            valid[i] = false;
        }

        Path path = new Path(baseDir + "/remaining.qid");
        try {
            FSDataInputStream in = fs.open(path);
            int length = in.readInt();
            for (int i = 0; i < length; i++) {
                valid[in.readInt()] = true;
            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
            LSHTool.printAndExit("read remaining query IDs failed");
        }
    }

    /**
     * map.
     * @param key dummy
     * @param value contains a block ID of an lshBlock
     * @param context
     * */
    @Override
    protected void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        int blockID = this.parseBlockID(value);

        // read lsh parameters
        LSHBlock lshBlock = new LSHBlock(blockID, dimension);
        lshBlock.readFromHdfs(baseDir, fs);
        
        // read a block of hash tables
        HashTableBlock tableBlock = new HashTableBlock(radiusID, blockID);
        tableBlock.readFromHdfs(baseDir, fs);

        int blockSize = tableBlock.getBlockSize();
        
        for (int i = 0; i < querySet.length; i++) {
            // i is the query ID
            if (!valid[i]) {
                // the i-th query has collected enough near neighbors
                continue;
            }
            for (int j = 0; j < blockSize; j++) {
                // j is the block table ID
                int bucketID = lshBlock.calcHashValue(j, radius, querySet[i]);
                SimpleList indexList = tableBlock.getBucket(j, bucketID);
                
                // No matter whether the bucket is empty or not, we will emit
                // all the queries that collide with it. Thus those queries that
                // collide with no data points in the current search radius will
                // be saved in the intermediate directory in CheckCandidateReducer, 
                // although actually there is no intermediate results. This
                // procedure is important since in this way these queries will
                // be processed again in the next search radius.
                if (null == indexList) {
                    indexList = emptyList;
                }

                queryIDWritable.set(i);     // set query id
                context.write(queryIDWritable, indexList);
            }
        }
    }
    
    /**
     * Parse the block ID.
     * @param value contains the block ID
     * */
    private int parseBlockID(final Text value) {
        int blockID = -1;
        Scanner scanner = null;
        try {
            scanner = new Scanner(value.toString());
            blockID = scanner.nextInt();
        } finally {
            scanner.close();
        }
        return blockID;
    }
}
