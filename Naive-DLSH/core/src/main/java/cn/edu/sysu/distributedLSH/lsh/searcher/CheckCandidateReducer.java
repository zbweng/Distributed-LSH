package cn.edu.sysu.distributedLSH.lsh.searcher;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.sysu.distributedLSH.common.CandidatePriorityQueue;


public class CheckCandidateReducer
extends Reducer<IntWritable, CandidatePriorityQueue, Object, CandidatePriorityQueue> {
    private Configuration conf;

    private int kNeighbors;
    private int radiusID;
    private String baseDir;

    // statistics
    private int dimension;
    private int nRadii;


    /**
     * setup.
     * @param context
     * */
    @Override
    protected void setup(final Context context) {
        conf = context.getConfiguration();

        kNeighbors = conf.getInt("kNeighbors", 0);
        radiusID = conf.getInt("radiusID", -1);
        baseDir = conf.get("baseDir");

        dimension = conf.getInt("dimension", 0);
        nRadii = conf.getInt("nRadii", 0);
    }
    

    /**
     * Reduce.
     * @param key queryID
     * @param values
     * @param context
     * */
    @Override
    protected void reduce(final IntWritable key, final Iterable<CandidatePriorityQueue> values,
            final Context context) throws IOException, InterruptedException {
        int queryID = key.get();

        CandidatePriorityQueue candPriQueue =
            new CandidatePriorityQueue(dimension, queryID, kNeighbors);
        if (radiusID > 0) {
            // Read intermediate result from the previous radius,
            // whose id is radiusID-1.
            candPriQueue.readFromHdfs(radiusID - 1, baseDir, conf);
        }

        // merge the search results from all data set splits
        for (CandidatePriorityQueue splitCand : values) {
            candPriQueue.merge(splitCand);
        }

        if (candPriQueue.size() == kNeighbors || radiusID + 1 == nRadii) {
            // If we have collect the top-k neighbors for this query or this
            // is the last radius, we will save the result to the final output.
            context.write(null, candPriQueue);
        } else {
            candPriQueue.saveToHdfs(radiusID, baseDir, conf);
        }
    }
}
