package cn.edu.sysu.distributedLSH.lsh.builder;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

import cn.edu.sysu.distributedLSH.common.IntPair;
import cn.edu.sysu.distributedLSH.common.IntTriple;
import cn.edu.sysu.distributedLSH.common.LSHBlock;


public class HashMapper extends Mapper<Object, Text, IntPair, IntTriple> {
    private static final int THRESHOLD_RADIUS = 1;
    private Configuration conf;
    private FileSystem fs;

    private int ratio;
    private int dimension;
    private int nRadii;
    private String baseDir;

    private LSHBlock[] lshBlocks = null;
    private int[] blockSizeArr = null;
    private int[] point = null;         // data point
    private int[] radii = null;

    // first: the radius ID
    // second: the block ID
    private IntPair intPair = new IntPair();

    // first: the table ID in a block
    // second: the bucket ID in a hash table
    // third: the index of the data point
    private IntTriple intTriple = new IntTriple();


    /**
     * setup.
     * @param context
     * */
    @Override
    protected void setup(final Context context) {
        try {
            conf = context.getConfiguration();
            fs = FileSystem.get(conf);

            ratio = conf.getInt("ratio", 0);
            dimension = conf.getInt("dimension", 0);
            nRadii = conf.getInt("nRadii", 0);
            baseDir = conf.get("baseDir");

            this.readLSHBlocks();
            
            // initialize multiple radii
            radii = new int[nRadii];
            radii[0] = THRESHOLD_RADIUS;
            for (int i = 1; i < nRadii; i++) {
                radii[i] = ratio * radii[i - 1];
            }

            // new an int array to store data point
            point = new int[dimension];
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * read LSH blocks from hdfs.
     * Although we use the same LSH for multiple radii, we divide the LSH
     * into several blocks.
     * @throws IOException 
     * */
    private void readLSHBlocks() throws IOException {
        PathFilter fileFilter = new PathFilter() {
            public boolean accept(Path path) {
                return path.getName().endsWith(".lshBlock");
            }
        };
        
        Path paramPath = new Path(baseDir + "/hashParam");
        FileStatus[] fileStatus = fs.listStatus(paramPath, fileFilter);
        int blockID;

        lshBlocks = new LSHBlock[fileStatus.length];
        blockSizeArr = new int[fileStatus.length];
        
        for (int i = 0; i < fileStatus.length; i++) {
            blockID = parseBlockID(fileStatus[i].getPath().getName());
            lshBlocks[blockID] = new LSHBlock(blockID, dimension);
            lshBlocks[blockID].readFromHdfs(baseDir, fs);
            blockSizeArr[blockID] = lshBlocks[blockID].getBlockSize();
        }
    }
    
    /**
     * Given the file name, parse the block ID.
     * @param fileName
     * */
    private int parseBlockID(final String fileName) {
        int dotIndex = fileName.indexOf(".");

        return Integer.parseInt(fileName.substring(0, dotIndex));
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
        int bucketID;
        int index = -1;     // the index of the data point
        Scanner scanner = null;

        try {
            scanner = new Scanner(value.toString());
            // Attention!
            // We count from zero while the input data set count from one.
            index = scanner.nextInt() - 1;
            for (int i = 0; i < dimension; i++) {
                point[i] = scanner.nextInt();
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            e.printStackTrace();
        } finally {
            scanner.close();
        }

        for (int i = 0; i < nRadii; i++) {
            // i is the radius id
            for (int j = 0; j < lshBlocks.length; j++) {
                // j is the block ID
                intPair.set(i, j);
                for (int k = 0; k < blockSizeArr[j]; k++) {
                    // k is the block table ID
                    bucketID = lshBlocks[j].calcHashValue(k, radii[i], point);
                    intTriple.set(k, bucketID, index);
                    context.write(intPair, intTriple);
                }
            }
        }
    }
}
