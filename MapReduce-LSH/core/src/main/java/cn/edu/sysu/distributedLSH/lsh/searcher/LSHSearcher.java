package cn.edu.sysu.distributedLSH.lsh.searcher;

import static java.lang.Math.*;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

import cn.edu.sysu.distributedLSH.common.Candidate;
import cn.edu.sysu.distributedLSH.common.LSHTool;


public class LSHSearcher extends Configured {
    private Configuration conf;
    private FileSystem fs;

    private int partNum;
    private int querySetSize;
    // the number of nearest neighbors that is required to find
    private int kNeighbors;

    // statistics
    private int dimension = -1;
    private int maxCoordinate = -1;
    private int nRadii = -1;

    private String baseDir;
    private String groundTruthFileName;

    private double[][] groundTruth = null;
    private double[][] searchResult = null;
    private Path querySeedPath;


    /**
     * Constructor.
     * */
    public LSHSearcher(final Configuration conf, final FileSystem fs) {
        this.conf = conf;
        this.fs = fs;

        partNum = conf.getInt("partNum", -1);
        if (partNum < 1) {
            LSHTool.printAndExit("partNum error");
        }

        querySetSize = conf.getInt("querySetSize", -1);
        if (querySetSize < 1) {
            LSHTool.printAndExit("querySetSize error");
        }

        kNeighbors = conf.getInt("kNeighbors", -1);
        if (kNeighbors < 1) {
            LSHTool.printAndExit("kNeighbors error");
        }

        baseDir = conf.get("baseDir");
        groundTruthFileName = conf.get("groundTruthFileName");
    }

    /**
     * run.
     * */
    public int run() throws IOException, InterruptedException, ClassNotFoundException {
        long startMillis;
        int totalSecond;

        startMillis = System.currentTimeMillis();
        
        this.readStatistics();

        // set some parameters which will be used in Mapper and Reducer
        conf.setInt("dimension", dimension);
        conf.setInt("nRadii", nRadii);

        this.createSeed();
        this.search();

        totalSecond = (int)((System.currentTimeMillis() - startMillis) / 1000.0);
        System.out.printf("---------------------------------------------------------------\n");
        System.out.printf("Data Set: %s, K: %s, Time of querying %d (sec) or %s\n", conf.get("dataset"),
                conf.get("kNeighbors"), totalSecond, LSHTool.convertTime(totalSecond));
        System.out.printf("---------------------------------------------------------------\n");

        this.compareToGroundTruth();

        return 0;
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
            nRadii = in.readInt();
            in.close();

            if (dimension < 1 || maxCoordinate < 1 || nRadii < 1) {
                LSHTool.printAndExit("statistics error");
            }
        } catch (IOException e) {
            e.printStackTrace();
            LSHTool.printAndExit("read statistics file error");
        }
    }
    
    /**
     * Create a query seed file for SearchMapper.
     * */
    private void createSeed() {
        String seedName = baseDir + "/query.seed";
        this.querySeedPath = new Path(seedName);

        try {
            fs.delete(querySeedPath, false);
            FSDataOutputStream out = fs.create(querySeedPath);
            for (int i = 0; i < partNum; i++) {
                // i is the partition id
                String str = i + "\n";
                out.write(str.getBytes());
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
            LSHTool.printAndExit("Create seed failed in LSHSearcher");
        }
    }
    
    /**
     * Search for the query set using MapReduce.
     * */
    private boolean search() throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(conf, "pdlsh LSHSearcher " + conf.get("dataset")
                + " K=" + conf.get("kNeighbors"));
        job.setJarByClass(LSHSearcher.class);
        job.setMapperClass(SearchMapper.class);
        job.setReducerClass(SearchReducer.class);

        job.setInputFormatClass(NLineInputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Candidate.class);

        job.setOutputFormatClass(SearchResultOutputFormat.class);

        NLineInputFormat.addInputPath(job, querySeedPath);
        String outputDir = baseDir + "/searchOutput";
        Path outPath = new Path(outputDir);
        fs.delete(outPath, true);
        FileOutputFormat.setOutputPath(job, outPath);

        return job.waitForCompletion(true);
    }

    /**
     * Read ground truth from hdfs.
     * */
    private void readGroundTruth() throws IOException {
        Path inputPath = new Path(groundTruthFileName);

        if (!fs.exists(inputPath)) {
            LSHTool.printAndExit("Input ground truth dose not exist");
        }
        if (!fs.isFile(inputPath)) {
            LSHTool.printAndExit("Input ground truth should be a file");
        }

        FSDataInputStream in = fs.open(inputPath);
        Scanner scanner = null;

        try {
            scanner = new Scanner(in, "UTF-8");

            if (scanner.nextInt() < querySetSize) {
                LSHTool.printAndExit("Require more exact distances for queries in ground truth");
            }

            int maxK = scanner.nextInt();
            if (maxK < kNeighbors) {
                LSHTool.printAndExit("The ground truth does not provide enough near neighbors");
            }

            // initialize groundTruth
            groundTruth = new double[querySetSize][];
            for (int i = 0; i < querySetSize; i++) {
                groundTruth[i] = new double[maxK];
            }

            for (int i = 0; i < querySetSize; i++) {
                // skip query ID
                scanner.nextInt();
                for (int j = 0; j < maxK; j++) {
                    groundTruth[i][j] = scanner.nextDouble();
                }
            }
        } finally {
            scanner.close();
        }
    }

    /**
     * Collect search results from output directories.
     * */
    private void collectSearchResult() throws IOException {
        PathFilter reduceFileFilter = new PathFilter() {
            public boolean accept(Path path) {
                return path.getName().startsWith("part");
            }
        };

        // initialize searchResult
        searchResult = new double[querySetSize][];
        String outputDir = baseDir + "/searchOutput";
        Path outPath =  new Path(outputDir);
        FileStatus[] fileStatus = fs.listStatus(outPath, reduceFileFilter);

        for (int i = 0; i < fileStatus.length; i++) {
            if (0 == fileStatus[i].getLen()) {
                continue;
            }

            FSDataInputStream in = fs.open(fileStatus[i].getPath());
            Scanner scanner = null;
            try {
                scanner = new Scanner(in, "UTF-8");

                while (scanner.hasNext()) {
                    int queryID = scanner.nextInt();
                    int neighborsFound = scanner.nextInt();

                    searchResult[queryID] = new double[neighborsFound];
                    for (int j = 0; j < neighborsFound; j++) {
                        searchResult[queryID][j] = scanner.nextDouble();
                        // skip data point
                        for (int dummy = 0; dummy < dimension; dummy++) {
                            scanner.nextInt();
                        }
                    }
                }
            } finally {
                scanner.close();
            }
        }
    }

    /**
     * Compare search results with the ground truth.
     * */
    private void compareToGroundTruth() throws IOException {
        this.readGroundTruth();
        this.collectSearchResult();

        String localStatResultFile = conf.get("localStatResultFile") + conf.get("kNeighbors");
        FileOutputStream fileOut = new FileOutputStream(localStatResultFile);
        BufferedOutputStream buffOut = new BufferedOutputStream(fileOut);
        PrintWriter writer = new PrintWriter(buffOut);

        try {
            double currentRatio, avgRatio;
            int missQuery = 0;

            avgRatio = 0;
            for (int i = 0; i < querySetSize; i++) {
                writer.printf("Query ID: %d\t\tNeighbors Found: %d", i, searchResult[i].length);
                if (kNeighbors != searchResult[i].length) {
                    missQuery++;
                    writer.printf(" (missing)");
                }
                writer.printf("\n");

                currentRatio = 0;
                for (int j = 0; j < searchResult[i].length; j++) {
                    writer.printf("%f ", searchResult[i][j]);
                    currentRatio += searchResult[i][j] / groundTruth[i][j];
                }
                currentRatio /= searchResult[i].length;
                if (currentRatio > 1 || abs(currentRatio - 1) < 0.00001) {
                    writer.printf("\nRatio: %f\n\n", currentRatio);
                } else {
                    writer.printf("\nRatio: %f (ERROR smaller than one)\n\n", currentRatio);
                }
                avgRatio += currentRatio;
            }

            avgRatio /= querySetSize;
            writer.printf("\nAverage Ratio: %f\n", avgRatio);
            writer.printf("Miss query: %d\n", missQuery);

            System.out.printf("\nAverage Ratio: %f\n", avgRatio);
            System.out.printf("Miss query: %d\n", missQuery);
            System.out.printf("Generate local statistical file successfully\n\n");
        } finally {
            writer.close();
        }
    }
}
