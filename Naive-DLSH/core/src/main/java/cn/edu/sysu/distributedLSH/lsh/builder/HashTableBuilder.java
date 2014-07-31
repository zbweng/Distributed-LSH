package cn.edu.sysu.distributedLSH.lsh.builder;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.edu.sysu.distributedLSH.common.IntPair;
import cn.edu.sysu.distributedLSH.common.IntTriple;
import cn.edu.sysu.distributedLSH.common.LSH;
import cn.edu.sysu.distributedLSH.common.LSHTool;


public class HashTableBuilder extends Configured {
    private Configuration conf;
    private FileSystem fs;

    private int ratio;
    private String baseDir;
    private String dataSetFileName;

    private int blockNum;   // the number of blocks

    // statistics
    private int dimension;
    private int maxCoordinate;
    private int dataSetSize;
    private int nRadii;


    /**
     * Constructor.
     * */
    public HashTableBuilder(final Configuration conf, final FileSystem fs) {
        this.conf = conf;
        this.fs = fs;

        ratio = conf.getInt("ratio", 0);
        if (ratio < 1) {
            LSHTool.printAndExit("ratio error in HashTableBuilder");
        }

        baseDir = conf.get("baseDir");
        dataSetFileName = conf.get("dataSetFileName");

        blockNum = conf.getInt("blockNum", 0);
        if (blockNum < 1) {
            System.out.println(blockNum);
            LSHTool.printAndExit("mapTasksNum error in HashTableBuilder");
        }

        this.readStatistics();

        // set some parameters which will be used in Mapper and Reducer
        conf.setInt("dimension", dimension);
        conf.setInt("nRadii", nRadii);
    }
    
    /**
     * read the final statistics file from hdfs.
     * */
    private void readStatistics() {
        String statFile = baseDir + "/stat/final.stat";
        Path statPath = new Path(statFile);

        try {
            if (!fs.isFile(statPath)) {
                LSHTool.printAndExit("statistics file does not exist");
            }
            FSDataInputStream in = fs.open(statPath);

            // The following variables are output to hdfs by Statistician.
            dimension = in.readInt();
            maxCoordinate = in.readInt();
            dataSetSize = in.readInt();
            nRadii = in.readInt();
            in.close();

            if (dimension < 1 || maxCoordinate < 1 || dataSetSize < 1 || nRadii < 1) {
                LSHTool.printAndExit("statistics error");
            }
        } catch (IOException e) {
            e.printStackTrace();
            LSHTool.printAndExit("read statistics file error");
        }
    }
    
    /**
     * run.
     * */
    public int run() throws IOException, InterruptedException, ClassNotFoundException, Exception {
        long startMillis;
        int totalSecond;

        startMillis = System.currentTimeMillis();
        try {
            this.build();
            this.hash();
        } catch (IOException e) {
            e.printStackTrace();
        }

        totalSecond = (int)((System.currentTimeMillis() - startMillis) / 1000.0);
        System.out.printf("---------------------------------------------------------------\n");
        System.out.printf("Data Set: %s, Time of building hash talbe %s\n", conf.get("dataset"),
                LSHTool.convertTime(totalSecond));
        System.out.printf("---------------------------------------------------------------\n");

        return 0;
    }

    /**
     * Build a LSH instance then save it to hdfs. Here, we use the same LSH
     * for multiple radii.
     * */
    private void build() throws IOException {
        LSH lsh = new LSH();

        lsh.calcParameters(maxCoordinate, dimension, dataSetSize, ratio, blockNum);
        lsh.saveAllBlocks(baseDir, fs);
    }

    /**
     * Hash the points in the data set to hash tables using MapReduce.
     * */
    private boolean hash() throws IOException, InterruptedException, ClassNotFoundException {
        Path inputPath = new Path(dataSetFileName);

        if (!fs.exists(inputPath)) {
            LSHTool.printAndExit("Input data set dose not exist");
        }
        if (!fs.isFile(inputPath)) {
            LSHTool.printAndExit("Input data set should be a file");
        }

        Job job = new Job(conf, "ndlsh HashTableBuilder " + conf.get("dataset"));
        job.setJarByClass(HashTableBuilder.class);
        job.setMapperClass(HashMapper.class);
        job.setReducerClass(HashReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(IntTriple.class);

        // TODO
        job.setSpeculativeExecution(false);

        TextInputFormat.addInputPath(job, inputPath);
        String outFile = baseDir + "/buildOutput";
        FileOutputFormat.setOutputPath(job, new Path(outFile));

        return job.waitForCompletion(true);
    }
}
