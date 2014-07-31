package cn.edu.sysu.distributedLSH.lsh.searcher;

import static java.lang.Math.abs;

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

import cn.edu.sysu.distributedLSH.common.CandidatePriorityQueue;
import cn.edu.sysu.distributedLSH.common.LSHTool;
import cn.edu.sysu.distributedLSH.common.SimpleList;


public class LSHSearcher extends Configured {
    private Configuration conf;
    private FileSystem fs;

    private int querySetSize;
    private int kNeighbors;
    private int blockNum;

    private String baseDir;
    private String groundTruthFileName;

    // statistics
    private int dimension;
    private int dataSetSize;
    private int nRadii;

    private int splitNum;
    private double[][] groundTruth = null;
    private double[][] searchResult = null;

    private Path blockSeedPath;
    private Path splitSeedPath;
    private Path interPath;     // the path where the intermediate search result is 


    /**
     * Constructor.
     * */
    public LSHSearcher(final Configuration conf, final FileSystem fs) {
        this.conf = conf;
        this.fs = fs;

        querySetSize = conf.getInt("querySetSize", 0);
        kNeighbors = conf.getInt("kNeighbors", 0);
        blockNum = conf.getInt("blockNum", 0);
        
        baseDir = conf.get("baseDir");
        groundTruthFileName = conf.get("groundTruthFileName");
        
        this.readStatistics();

        // set some parameters which will be used in Mapper and Reducer
        conf.setInt("dimension", dimension);
        conf.setInt("dataSetSize", dataSetSize);
        conf.setInt("nRadii", nRadii);
        
        try {
            int hashTableSize = this.getHashTableSize();
            if (0 == hashTableSize) {
                LSHTool.printAndExit("hashTableSize can not be zero in LSHSearcher");
            }
            conf.setInt("hashTableSize", hashTableSize);
        } catch (IOException e) {
            e.printStackTrace();
            LSHTool.printAndExit("get hashTableSize failed");
        }
        
        this.handleSplits();
        this.createBlockSeed();
    }
    
    /**
     * Get the number of splits of the input data set. Then create a
     * split seed file.
     * */
    private void handleSplits() {
        try {
            String dir = baseDir + "/dataset";
            Path path =  new Path(dir);
            FileStatus[] fileStatus = fs.listStatus(path);

            splitNum = fileStatus.length;
            if (0 == splitNum) {
                LSHTool.printAndExit("splitNum can not be zero in LSHSearcher");
            }
            conf.setInt("splitNum", splitNum);

            // Create a split seed file for CheckCandidateMapper.
            String fileName = baseDir + "/split.seed";
            this.splitSeedPath = new Path(fileName);

            fs.delete(splitSeedPath, false);
            FSDataOutputStream out = fs.create(splitSeedPath);

            for (int i = 0; i < fileStatus.length; i++) {
                int startID = this.parseStartID(fileStatus[i].getPath().getName());
                String str = startID + "\n";
                out.write(str.getBytes());
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
            LSHTool.printAndExit("Failed in LSHSearcher handleSplits");
        }
    }
    
    /**
     * Given the file name, parse the start ID.
     * @param fileName format as "startID.split"
     * */
    private int parseStartID(final String fileName) {
        int dotIndex = fileName.indexOf(".");

        return Integer.parseInt(fileName.substring(0, dotIndex));
    }
    
    /**
     * Read the size of all blocks from hdfs then sum up to get
     * the total hash table size.
     * @throws IOException 
     */
    private int getHashTableSize() throws IOException {
        String inFile = baseDir + "/hashParam/blockSize.info";
        Path inPath = new Path(inFile);
        int hashTableSize = 0;

        FSDataInputStream in = fs.open(inPath);
        int length = in.readInt();

        for (int i = 0; i < length; i++) {
            hashTableSize += in.readInt();
        }
        in.close();

        return hashTableSize;
    }
    
    /**
     * read the final statistics file from hdfs.
     * */
    private void readStatistics() {
        String statFile = baseDir + "/stat/final.stat";
        Path statPath = new Path(statFile);
        // parameters that we do not need
        int maxCoordinate;

        try {
            if (!fs.isFile(statPath)) {
                LSHTool.printAndExit(statFile + " does not exist in LSHSearcher");
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
     * Create a query seed file for CollectCandidateMapper.
     * */
    private void createBlockSeed() {
        String fileName = baseDir + "/block.seed";
        this.blockSeedPath = new Path(fileName);

        try {
            fs.delete(blockSeedPath, false);
            FSDataOutputStream out = fs.create(blockSeedPath);
            for (int i = 0; i < blockNum; i++) {
                // i is the block ID
                String str = i + "\n";
                out.write(str.getBytes());
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
            LSHTool.printAndExit("Create block.seed failed in LSHSearcher");
        }
    }
    
    /**
     * run.
     * */
    public int run() throws IOException, InterruptedException, ClassNotFoundException, Exception {
        long startMillis;
        int totalSecond;

        startMillis = System.currentTimeMillis();

        // search in the first radius using MapReduce
        this.search(0);

        int searchCount = 1;      // how many radius that has been searched

        if (fs.exists(interPath)) {
            // We can not find enough near neighbors for some queries in the
            // first radius thus we should expand the search radius.
            while (searchCount < nRadii) {
                // Those queries that can not find enough near neighbors in the previous
                // radius will be searched again in the current radius.
                this.saveRemainingQueryID();

                // search in the current radius using MapReduce
                this.search(searchCount);
                searchCount++;

                if (!fs.exists(interPath)) {
                    // There is no intermediate results generated thus the search
                    // procedure completes.
                    break;
                }
            }
        }

        totalSecond = (int)((System.currentTimeMillis() - startMillis) / 1000.0);
        System.out.printf("---------------------------------------------------------------\n");
        System.out.printf("Data Set: %s, K: %s, Time of querying %d (sec) or %s\n", conf.get("dataset"),
                conf.get("kNeighbors"), totalSecond, LSHTool.convertTime(totalSecond));
        System.out.printf("---------------------------------------------------------------\n");

        this.compareToGroundTruth(searchCount);

        return 0;
    }
    
    /**
     * Search using MapReduce.
     * @param radiusID
     * */
    private void search(final int radiusID) throws IOException, InterruptedException,
    ClassNotFoundException, Exception {
        boolean flag;

        // clear the intermediate directory in current radius
        String interDirName = baseDir + "/radius_" + radiusID + "/intermediate";
        interPath = new Path(interDirName);
        fs.delete(interPath, true);

        flag = this.collectCandidate(radiusID);
        if (!flag) {
            LSHTool.printAndExit("collectCandidate failed with radiusID: " + radiusID);
        }

        flag = this.checkCandidate(radiusID);
        if (!flag) {
            LSHTool.printAndExit("checkCandidate failed with radiusID: " + radiusID);
        }
    }
    
    
    /**
     * Collect candidates for the query set using MapReduce.
     * @param radiusID
     * */
    private boolean collectCandidate(final int radiusID) 
            throws IOException, InterruptedException, ClassNotFoundException {
        // set the radius ID
        conf.set("radiusID", String.valueOf(radiusID));

        Job job = new Job(conf, "ndlsh CollectCandidate " + conf.get("dataset")
                + " K=" + conf.get("kNeighbors") + " radiusID=" + radiusID);

        job.setJarByClass(LSHSearcher.class);
        job.setMapperClass(CollectCandidateMapper.class);
        // TODO Combiner is not suitable here since DLSH is cpu bound thus we delete SearchCombiner.
        // job.setCombinerClass(SearchCombiner.class);
        job.setReducerClass(CollectCandidateReducer.class);
        job.setNumReduceTasks(1);

        job.setInputFormatClass(NLineInputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SimpleList.class);

        NLineInputFormat.addInputPath(job, blockSeedPath);
        String outputDir = baseDir + "/collectCandOutput";
        Path outPath = new Path(outputDir);
        fs.delete(outPath, true);
        FileOutputFormat.setOutputPath(job, outPath);

        return job.waitForCompletion(true);
    }
    
    private boolean checkCandidate(final int radiusID)
            throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(conf, "ndlsh CheckCandidate " + conf.get("dataset")
                + " K=" + conf.get("kNeighbors") + " radiusID=" + radiusID);

        job.setJarByClass(LSHSearcher.class);
        job.setMapperClass(CheckCandidateMapper.class);
        job.setReducerClass(CheckCandidateReducer.class);
        
        job.setInputFormatClass(NLineInputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(CandidatePriorityQueue.class);

        job.setOutputFormatClass(SearchResultOutputFormat.class);

        NLineInputFormat.addInputPath(job, splitSeedPath);
        String outputDir = baseDir + "/checkCandOutput_" + radiusID;
        Path outPath = new Path(outputDir);
        fs.delete(outPath, true);
        FileOutputFormat.setOutputPath(job, outPath);

        return job.waitForCompletion(true);
    }

    /**
     * Get all files in interPath and parse them to integers. Actually,
     * the file names are the remaining query IDs. Then save the remaining
     * query IDs to hdfs.
     * */
    private void saveRemainingQueryID() throws IOException {
        Path path = new Path(baseDir + "/remaining.qid");
        fs.delete(path, false);
        FSDataOutputStream out = fs.create(path);
        FileStatus[] fileStatus = fs.listStatus(interPath);

        out.writeInt(fileStatus.length);
        for (int i = 0; i < fileStatus.length; i++) {
            int queryID = Integer.parseInt(fileStatus[i].getPath().getName());
            out.writeInt(queryID);
        }
        out.close();
    }

    /**
     * Read ground truth from hdfs.
     * */
    private void readGroundTruth() throws IOException {
        if (0 == querySetSize || 0 == kNeighbors) {
            LSHTool.printAndExit("querySetSize and kNeighbors should not be zero");
        }

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
     * @param searchCount how many radius that has been searched
     * */
    private void collectSearchResult(final int searchCount) throws IOException {
        PathFilter reduceFileFilter = new PathFilter() {
            public boolean accept(Path path) {
                return path.getName().startsWith("part");
            }
        };

        // initialize searchResult
        searchResult = new double[querySetSize][];

        for (int i = 0; i < searchCount; i++) {
            String outputDir = baseDir + "/checkCandOutput_" + i;
            Path outPath =  new Path(outputDir);
            FileStatus[] fileStatus = fs.listStatus(outPath, reduceFileFilter);

            for (int j = 0; j < fileStatus.length; j++) {
                if (0 == fileStatus[j].getLen()) {
                    continue;
                }

                FSDataInputStream in = fs.open(fileStatus[j].getPath());
                Scanner scanner = null;
                try {
                    scanner = new Scanner(in, "UTF-8");

                    while (scanner.hasNext()) {
                        int queryID = scanner.nextInt();
                        int neighborsFound = scanner.nextInt();

                        searchResult[queryID] = new double[neighborsFound];
                        for (int k = 0; k < neighborsFound; k++) {
                            searchResult[queryID][k] = scanner.nextDouble();
                            // skip data point index
                            scanner.nextInt();
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
    }

    /**
     * Compare search results with the ground truth.
     * @param searchCount how many radius that has been searched
     * */
    private void compareToGroundTruth(final int searchCount) throws IOException {
        System.out.printf("Total search radii: %d\n", searchCount);

        this.readGroundTruth();
        this.collectSearchResult(searchCount);

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
