package cn.edu.sysu.distributedLSH.common;

import static java.lang.Math.*;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class LSH {
    /** CONSTANT VARIABLES */
    private static final double LOG2 = log(2.0);
    // width of the interval, or the "bucket", that is w
    private static final double WIDTH = 4.0;

    /** parameters from input */
    private int maxCoordinate;      // that is t
    private int dim = -1;           // dimensionality, that is d
    private int cardinality;        // cardinality of the data set, that is n
    private int ratio = -1;         // approximation ratio

    // max value of the shifted projection, max(a*v + b), that is U
    // Each dimension has domain [-U/2, U/2].
    private double maxShiftedProj = -1;
    // probability of objects of radius <= 1 which are hashed to the same
    // buckets
    private double p1;
    // probability of objects of radius >= c which are hashed to the same
    // buckets
    private double p2;

    private int projDim;              // dimensionality after projection, that is m
    private int hashTableSize;        // number of hash tables, that is L
    // how many bits are needed to represent a component in an original vector, that is f
    private int origVecBitWidth;
    // how many bits are needed to represent a component in the hashed vector, that is u
    private int hashVecBitWidth = -1;
    
    private LSHBlock[] lshBlocks = null;


    /**
     * Constructor.
     * */
    public LSH() {}

    public int getRatio() {
        return ratio;
    }

    public int getHashTableSize() {
        return hashTableSize;
    }

    /**
     * Calculate parameters for LSH.
     * */
    public void calcParameters(final int maxCoordinate, final int dim,
            final int cardinality, final int ratio, final int blockNum) {
        this.maxCoordinate = maxCoordinate;
        this.dim = dim;
        this.cardinality = cardinality;
        this.ratio = ratio;

        origVecBitWidth = (int)ceil(log(dim)/LOG2 + log(maxCoordinate)/LOG2);
        if (origVecBitWidth > 60) {
            LSHTool.printAndExit("ERROR: origVecBitWidth (f) > 60, overflow may happen");
        }

        p1 = calcLshProbability(WIDTH);
        p2 = calcLshProbability(WIDTH / ratio);

        this.calcProjectionDim();
        this.calcHashTableSize();

        this.divideHashTables(blockNum);
        
        this.calcHashVecBitWidth();
        this.calcMaxShiftedProj();

        System.out.printf("Parameters:\n");
        System.out.printf("\torigVecBitWidth (f) = %d\n", origVecBitWidth);
        System.out.printf("\tp1 = %.9f\n", p1);
        System.out.printf("\tp2 = %.9f\n", p2);
        System.out.printf("\tprojDim (m) = %d\n", projDim);
        System.out.printf("\thashTableSize (L) = %d\n", hashTableSize);
        System.out.printf("\thashVecBitWidth (u) = %d\n", hashVecBitWidth);
        System.out.printf("\tmaxShifted (U) = %.1f\n", maxShiftedProj);
    }

    /**
     * Calculate the probability according to p-Stable LSH.
     * */
    private static double calcLshProbability(final double x) {
        double prob = 1.0;

        prob -= 2.0 * LSHTool.standardNormalCdf(-x, 0.0001);
        prob -= (2.0 / (sqrt(2.0*PI) * x)) * (1.0 - exp(-(x*x) / 2.0));
        return prob;
    }

    /**
     * Calculate the dimensionality after projection.
     * m = log(n) / log(1/p2)
     * */
    private void calcProjectionDim() {
        projDim = (int)ceil(log(cardinality) / log(1.0/p2));
    }

    /**
     * Calculate the size of the hash tables.
     * L = 1 / p1^m
     * */
    private void calcHashTableSize() {
        hashTableSize = (int)ceil(1.0 / pow(p1, projDim));
    }
    
    /**
     * Divide the whole hash tables into several blocks. This is the basic
     * idea to do distributed computing.
     * @param blockNum
     * */
    private void divideHashTables(final int blockNum) {
        int blockSize = hashTableSize / blockNum;

        if (blockSize < 1) {
            LSHTool.printAndExit("blockSize can not be smaller than 1");
        }
        
        lshBlocks = new LSHBlock[blockNum];
        
        for (int i = 0; i < lshBlocks.length - 1; i++) {
            // i is the block ID
            lshBlocks[i] = new LSHBlock(i, blockSize, origVecBitWidth,
                    maxCoordinate, dim, projDim);
        }
        lshBlocks[lshBlocks.length - 1] = new LSHBlock(lshBlocks.length - 1,
                hashTableSize - (lshBlocks.length - 1) * blockSize,
                origVecBitWidth, maxCoordinate, dim, projDim);
    }
    
    /**
     * Calculate the hashVecBitWidth.
     * */
    private void calcHashVecBitWidth() {
        double maxHashValue, hashValue;

        maxHashValue = pow(2, origVecBitWidth);
        for (int i = 0; i < lshBlocks.length; i++) {
            hashValue = lshBlocks[i].calcMaxHashValue();
            if (hashValue > maxHashValue) {
                maxHashValue = hashValue;
            }
        }
        
        hashVecBitWidth = (int)ceil(log(maxHashValue)/LOG2 - 1) + 1;

        if (hashVecBitWidth > 30) {
            LSHTool.printAndExit("hashVecBitWidth is too large (>= 31).");
        }

        for (int i = 0; i < lshBlocks.length; i++) {
            lshBlocks[i].setHashVecBitWidth(hashVecBitWidth);
        }
    }
    
    /**
     * Calculate the maxShiftedProj.
     * */
    private void calcMaxShiftedProj() {
        maxShiftedProj = (1 << hashVecBitWidth) * WIDTH;

        for (int i = 0; i < lshBlocks.length; i++) {
            lshBlocks[i].setMaxShiftedProj(maxShiftedProj);
        }
    }
    
    /**
     * Save all LSH blocks to hdfs.
     * */
    public void saveAllBlocks(final String baseDir, final FileSystem fs) throws IOException {
        for (int i = 0; i < lshBlocks.length; i++) {
            lshBlocks[i].saveToHdfs(baseDir, fs);
        }

        // save the size of all blocks to hdfs
        String outFile = baseDir + "/hashParam/blockSize.info";
        Path outPath = new Path(outFile);

        fs.delete(outPath, false);

        FSDataOutputStream out = fs.create(outPath);
        try {
            out.writeInt(lshBlocks.length);
            for (int i = 0; i < lshBlocks.length; i++) {
                out.writeInt(lshBlocks[i].getBlockSize());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            out.close();
        }
    }
}
