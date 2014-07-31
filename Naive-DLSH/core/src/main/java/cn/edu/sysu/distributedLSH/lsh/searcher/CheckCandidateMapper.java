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

import cn.edu.sysu.distributedLSH.common.CandidatePriorityQueue;
import cn.edu.sysu.distributedLSH.common.DatasetSplit;
import cn.edu.sysu.distributedLSH.common.LSHTool;


public class CheckCandidateMapper
extends Mapper<Object, Text, IntWritable, CandidatePriorityQueue> {
    private static final int THRESHOLD_RADIUS = 1;
    
    private Configuration conf;
    private FileSystem fs;

    private int ratio;
    private int dimension;
    private int querySetSize;
    private int kNeighbors;
    private int radiusID;

    private String baseDir;
    private String querySetFileName;

    private int ratioRadius;
    private int[][] querySet = null;
    private IntWritable queryIDWritable = new IntWritable();


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
        kNeighbors = conf.getInt("kNeighbors", 0);
        radiusID = conf.getInt("radiusID", -1);

        baseDir = conf.get("baseDir");
        querySetFileName = conf.get("querySetFileName");
        
        // ratio * the current radius
        ratioRadius = (int)(ratio * THRESHOLD_RADIUS * pow(ratio, radiusID));
        
        try {
            this.readQuerySet();
        } catch (IOException e) {
            e.printStackTrace();
            LSHTool.printAndExit("read query set failed");
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
     * map.
     * @param key dummy
     * @param value contains the start ID of a split
     * @param context
     * */
    @Override
    protected void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        int startID = this.parseStartID(value);
        DatasetSplit datasetSplit = new DatasetSplit();

        // read the split of the data set from hdfs
        datasetSplit.readFromHdfs(baseDir, fs, startID);
        int[][] split = datasetSplit.get();
        
        // read candidate indices file from hdfs
        String candFile = baseDir + "/splitCand/radius_" + radiusID + "/" + startID + ".cand";
        Path candPath = new Path(candFile);
        FSDataInputStream in = fs.open(candPath);
        
        int splitQuerySize = in.readInt();
        for (int i = 0; i < splitQuerySize; i++) {
            int queryID = in.readInt();
            CandidatePriorityQueue candPriQueue =
                    new CandidatePriorityQueue(dimension, queryID, kNeighbors);
            int candNum = in.readInt();

            for (int j = 0; j < candNum; j++) {
                int globalIndex = in.readInt();
                // relative index: the index of a data point in a split
                int relativeIndex = globalIndex - startID;
                candPriQueue.update(globalIndex, split[relativeIndex],
                        querySet[queryID], ratioRadius);
            }
            queryIDWritable.set(queryID);
            context.write(queryIDWritable, candPriQueue);
        }
    }
    
    /**
     * Parse the start ID.
     * @param value contains the start ID
     * */
    private int parseStartID(final Text value) {
        int startID = -1;
        Scanner scanner = null;
        try {
            scanner = new Scanner(value.toString());
            startID = scanner.nextInt();
        } finally {
            scanner.close();
        }
        return startID;
    }
}
