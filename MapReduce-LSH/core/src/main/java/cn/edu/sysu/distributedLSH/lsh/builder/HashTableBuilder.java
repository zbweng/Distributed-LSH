package cn.edu.sysu.distributedLSH.lsh.builder;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.edu.sysu.distributedLSH.common.LSHTool;


public class HashTableBuilder extends Configured {
    private Configuration conf;
    private FileSystem fs;

    // The original data set will be partitioned into partNum parts. Also,
    // mapred.reduce.tasks will be set to partNum when building hash tables.
    private int partNum;
    private String baseDir;
    private String dataSetFileName;


    /**
     * Constructor.
     * */
    public HashTableBuilder(final Configuration conf, final FileSystem fs) {
        this.conf = conf;
        this.fs = fs;

        partNum = conf.getInt("partNum", 0);
        if (0 == partNum) {
            LSHTool.printAndExit("partNum error");
        }
        baseDir = conf.get("baseDir");
        dataSetFileName = conf.get("dataSetFileName");
    }

    /**
     * run.
     * */
    public int run() throws IOException, InterruptedException, ClassNotFoundException {
        long startMillis;
        int totalSecond;

        startMillis = System.currentTimeMillis();
        this.build();
        totalSecond = (int)((System.currentTimeMillis() - startMillis) / 1000.0);

        System.out.printf("---------------------------------------------------------------\n");
        System.out.printf("Data Set: %s, Time of building hash talbe %s\n", conf.get("dataset"),
                LSHTool.convertTime(totalSecond));
        System.out.printf("---------------------------------------------------------------\n");

        return 0;
    }

    /**
     * Build LSH and hash tables. Then hash the data points to the built hash tables.
     * */
    private boolean build() throws IOException, InterruptedException, ClassNotFoundException {
        Path inputPath = new Path(dataSetFileName);
        
        if (!fs.exists(inputPath)) {
            LSHTool.printAndExit("Input data set dose not exist");
        }
        if (!fs.isFile(inputPath)) {
            LSHTool.printAndExit("Input data set should be a file");
        }

        // set timeout to 90 minutes
        conf.setLong("mapred.task.timeout", 5400000);

        Job job = new Job(conf, "pdlsh HashTableBuilder " + conf.get("dataset"));
        job.setJarByClass(HashTableBuilder.class);
        job.setMapperClass(HashMapper.class);
        job.setReducerClass(HashReducer.class);
        job.setNumReduceTasks(partNum);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // TODO
        job.setSpeculativeExecution(false);

        TextInputFormat.addInputPath(job, inputPath);
        String outFile = baseDir + "/buildOutput";
        FileOutputFormat.setOutputPath(job, new Path(outFile));

        return job.waitForCompletion(true);
    }
}
