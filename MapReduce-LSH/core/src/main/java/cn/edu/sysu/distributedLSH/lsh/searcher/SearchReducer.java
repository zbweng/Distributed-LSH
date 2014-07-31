package cn.edu.sysu.distributedLSH.lsh.searcher;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.sysu.distributedLSH.common.Candidate;
import cn.edu.sysu.distributedLSH.common.CandidatePriorityQueue;


public class SearchReducer extends Reducer<IntWritable, Candidate, IntWritable, CandidatePriorityQueue> {
    private Configuration conf;

    private int kNeighbors;


    /**
     * setup.
     * @param context
     * */
    @Override
    protected void setup(final Context context) {
        conf = context.getConfiguration();

        kNeighbors = conf.getInt("kNeighbors", -1);
    }

    /**
     * Reduce.
     * @param key contains the query id
     * @param values contains a list of candidates
     * @param context
     * */
    @Override
    public void reduce(final IntWritable key, final Iterable<Candidate> values,
            final Context context) throws IOException, InterruptedException {
        CandidatePriorityQueue candPriQueue = new CandidatePriorityQueue(kNeighbors);

        for (Candidate candidate : values) {
            candPriQueue.update(candidate);
        }
        context.write(key, candPriQueue);
    }
}
