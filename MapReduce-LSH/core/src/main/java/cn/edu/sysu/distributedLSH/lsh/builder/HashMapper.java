package cn.edu.sysu.distributedLSH.lsh.builder;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

import cn.edu.sysu.distributedLSH.common.LSHTool;


public class HashMapper extends Mapper<Object, Text, IntWritable, Text> {
    private static Random random = new Random();

    private Configuration conf;
    private int partNum;

    private IntWritable partID = new IntWritable();


    /**
     * setup.
     * @param context
     * */
    @Override
    protected void setup(final Context context) {
        conf = context.getConfiguration();

        partNum = conf.getInt("partNum", 0);
        if (0 == partNum) {
            LSHTool.printAndExit("partNum error");
        }
    }

    /**
     * map.
     * @param key dummy
     * @param value contains the data point
     * @param context
     * */
    @Override
    protected void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        // Partition the original data set into several parts. The part that a data point
        // is distributed to is selected with equal probability. Each part will be processed
        // by a reducer.
        partID.set(random.nextInt(partNum));
        context.write(partID, value);
    }
}
