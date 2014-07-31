package cn.edu.sysu.distributedLSH.common;

import static java.lang.Math.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;


public class LSH implements Writable {
    /** CONSTANT VARIABLES */
    private static final double LOG2 = log(2.0);
    private static final int MAX_HASH_BASE = 536870912;     // 2^29
    private static final long MASK = 4294967295L;           // 2^32 - 1
    private static final int PRIME = 2147483647;            // 2^31 - 1
    // width of the interval, or the "bucket", that is w
    private static final double WIDTH = 4.0;

    /** parameters from input */
    private int maxCoordinate;      // the maximum coordinate value in the data set, that is t
    private int dim = -1;           // dimensionality, that is d
    private int cardinality;        // cardinality of the data set, that is n
    private int ratio = -1;         // approximation ratio

    /** parameters for LSH */
    private double[][][] projVector = null;     // projection vector, that is a
    private double[][] shift = null;            // shifting parameter, that is b
    // max value of the shifted projection, max(a*v + b), that is U
    // Each dimension has domain [-U/2, U/2].
    private double maxShiftedProj;
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
    private int hashVecBitWidth;

    // standard hash to project an m-dimension vector to a value
    // in [0, cardinality]
    private int[] standardHash = null;


    /**
     * Constructor.
     * */
    public LSH(final int dim) {
        this.dim = dim;
    }

    public int getRatio() {
        return ratio;
    }

    public int getHashTableSize() {
        return hashTableSize;
    }

    /**
     * Calculate parameters for LSH.
     * */
    public void calcParameters(final int maxCoordinate, final int cardinality, final int ratio) {
        this.maxCoordinate = maxCoordinate;
        this.cardinality = cardinality;
        this.ratio = ratio;

        origVecBitWidth = (int)ceil(log(dim)/LOG2 + log(maxCoordinate)/LOG2);
        if (origVecBitWidth > 60) {
            LSHTool.printAndExit("ERROR: origVecBitWidth (f) > 60, overflow may happen");
        }

        p1 = calcLshProbability(WIDTH);
        p2 = calcLshProbability(WIDTH / ratio);

        projDim = calcProjectionDim();
        hashTableSize = calcHashTableSize();

        generateHashParameters();
        generateStandardHash();

        hashVecBitWidth = calcHashVecBitWidth();
        maxShiftedProj = (1 << hashVecBitWidth) * WIDTH;

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
    private int calcProjectionDim() {
        return (int)ceil(log(cardinality) / log(1.0/p2));
    }

    /**
     * Calculate the size of the hash tables.
     * L = 1 / p1^m
     * */
    private int calcHashTableSize() {
        return (int)ceil(1.0 / pow(p1, projDim));
    }

    /**
     * Generate projVector and shift, that is a and b respectively. They
     * are the parameters of the p-Stable LSH functions. 
     * Attention: shift is NOT chosen uniformly from the range [0, width]
     * as in the original paper. We amplify the range so that after we
     * enlarge the searching radius (or the "bucket" width) these shifting
     * parameters are also valid.
     * */
    private void generateHashParameters() {
        // Notice that maxShift must be a multiple of width.
        // Here, the long integer maxShift may overflow.
        final long maxShift = (1 << origVecBitWidth) * (long)WIDTH;

        if (null == projVector) {
            projVector = new double[hashTableSize][][];
            for (int i = 0; i < hashTableSize; i++) {
                projVector[i] = new double[projDim][];
                for (int j = 0; j < projDim; j++) {
                    projVector[i][j] = new double[dim];
                }
            }
        }

        for (int i = 0; i < hashTableSize; i++) {
            for (int j = 0; j < projDim; j++) {
                for (int k = 0; k < dim; k++) {
                    projVector[i][j][k] = LSHTool.generalGaussian(0.0, 1.0);
                }
            }
        }

        if (null == shift) {
            shift = new double[hashTableSize][];
            for (int i = 0; i < hashTableSize; i++) {
                shift[i] = new double[projDim];
            }
        }

        for (int i = 0; i < hashTableSize; i++) {
            for (int j = 0; j < projDim; j++) {
                shift[i][j] = LSHTool.boundedDigitUniform(0, maxShift);
            }
        }
    }

    /**
     * Generate standard hashing. They are used to compute the location of
     * buckets in all the hash tables.
     * */
    public void generateStandardHash() {
        if (null == standardHash) {
            standardHash = new int[projDim];
        }
        for (int i = 0; i < projDim; i++) {
            standardHash[i] = (int)LSHTool.generalUniform(1, MAX_HASH_BASE);
        }
    }

    /** Calculate the hashVecBitWidth. */
    private int calcHashVecBitWidth() {
        int result;
        double max, sum, hashValue;

        max = pow(2, origVecBitWidth);
        for (int i = 0; i < hashTableSize; i++)  {
            for (int j = 0; j < projDim; j++) {
                sum = 0;
                for (int k = 0; k < dim; k++) {
                    sum += abs(projVector[i][j][k]);
                }
                hashValue = 2 * (sum*maxCoordinate + shift[i][j]) / WIDTH;
                if (max < hashValue) {
                    max = hashValue;
                }
            }
        }
        result = (int)ceil(log(max) / LOG2 - 1) + 1;

        if (result > 30) {
            LSHTool.printAndExit("hashVecBitWidth is too large (>= 31).");
        }
        return result;
    }

    /**
     * Calculate hash value for a point in a hash table with ID tableID.
     * */
    public int calcHashValue(final int tableID, final int radius, final int[] point) {
        int result;
        double hashValue;
        double hashVector[] = new double[projDim];

        // Project a point to an m-dimension vector
        for (int i = 0; i < projDim; i++) {
            hashValue = 0;
            for (int j = 0; j < dim; j++) {
                hashValue += projVector[tableID][i][j] * point[j];
            }
            hashVector[i] = hashValue + shift[tableID][i];
        }
        // Call the auxiliary function to calculate standard hash value
        result = calcStandardHashValue(radius, hashVector);
        return result;
    }

    /**
     * Calculate standard hash value for an m-dimension hash vector. 
     * This is an auxiliary function for calcHashValue.
     * */
    private int calcStandardHashValue(final int radius, final double[] hashVector) {
        final int maxHashingValue = 1 << hashVecBitWidth;
        int shiftedVector[] = new int[projDim];

        // Shift the hash vector first.
        for (int i = 0; i < projDim; i++) {
            // Move the hash vector (maxShifted / 2) units towards right
            // to make it non-negative.
            shiftedVector[i] = (int)floor((hashVector[i] + maxShiftedProj/2.0) / (WIDTH*radius));

            if (shiftedVector[i] < 0 || shiftedVector[i] >= maxHashingValue) {
                System.out.printf("%d, %d, %.9f\n",
                        maxHashingValue, shiftedVector[i], hashVector[i]);
                LSHTool.printAndExit("Illegal coordinate in the hash space found.");
            }
        }

        // Then calculate standard hash value based on the shifted vector.
        long result = 0;
        for (int i = 0; i < projDim; i++) {
            result += shiftedVector[i] * standardHash[i];
            // (result & mask) equal to lower-32-bit of result
            // (result >> 32) equal to higher-32-bit of result
            result = (result & MASK) + 5 * (result >> 32);
            result %= PRIME;
        }

        return (int)result;
    }

    /**
     * hashCode.
     * */
    @Override
    public int hashCode() {
        return dim + cardinality;
    }

    /**
     * equals.
     * */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LSH) {
            LSH that = (LSH)obj;
            if (this.dim == that.dim && this.cardinality == that.cardinality) {
                if (this.standardHash.length == that.standardHash.length) {
                    for (int i = 0; i < this.standardHash.length; i++) {
                        if (this.standardHash[i] != that.standardHash[i]) {
                            return false;
                        }
                    }
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Implement the method in the interface Writable.
     * @param out output stream
     * */
    public void write(final DataOutput out) throws IOException {
        // write some int
        out.writeInt(hashTableSize);
        out.writeInt(projDim);
        out.writeInt(hashVecBitWidth);
        // write some double
        out.writeDouble(maxShiftedProj);

        // write projVector
        for (int i = 0; i < hashTableSize; i++) {
            for (int j = 0; j < projDim; j++) {
                for (int k = 0; k < dim; k++) {
                    out.writeDouble(projVector[i][j][k]);
                }
            }
        }
        // write shift
        for (int i = 0; i < hashTableSize; i++) {
            for (int j = 0; j < projDim; j++) {
                out.writeDouble(shift[i][j]);
            }
        }
        // write standardHash
        for (int i = 0; i < projDim; i++) {
            out.writeInt(standardHash[i]);
        }
    }

    /**
     * Implement the method in the interface Writable.
     * @param in input stream
     * */
    public void readFields(final DataInput in) throws IOException {
        // read some int
        hashTableSize = in.readInt();
        projDim = in.readInt();
        hashVecBitWidth = in.readInt();
        // read some double
        maxShiftedProj = in.readDouble();

        // read projVector
        projVector = new double[hashTableSize][][];
        for (int i = 0; i < hashTableSize; i++) {
            projVector[i] = new double[projDim][];
            for (int j = 0; j < projDim; j++) {
                projVector[i][j] = new double[dim];
                for (int k = 0; k < dim; k++) {
                    projVector[i][j][k] = in.readDouble();
                }
            }
        }
        // read shift
        shift = new double[hashTableSize][];
        for (int i = 0; i < hashTableSize; i++) {
            shift[i] = new double[projDim];
            for (int j = 0; j < projDim; j++) {
                shift[i][j] = in.readDouble();
            }
        }
        // read standardHash
        standardHash = new int[projDim];
        for (int i = 0; i < projDim; i++) {
            standardHash[i] = in.readInt();
        }
    }

    /**
     * Save the LSH to hdfs.
     * @param dir the directory
     * @param conf
     * */
    public void saveToHdfs(final String dir, final FileSystem fs)
            throws IOException {
        String fileName = dir + "/hash.param";

        Path outFile = new Path(fileName);
        if (fs.exists(outFile)) {
            LSHTool.printAndExit("Output file " + fileName + " already exists");
        }

        FSDataOutputStream out = fs.create(outFile);
        try {
            this.write(out);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            out.close();
        }
    }

    /**
     * Read the LSH from hdfs.
     * @param dir the directory
     * @param conf
     * */
    public void readFromHdfs(final String dir, final FileSystem fs) throws IOException {
        String fileName = dir + "/hash.param";

        Path inFile = new Path(fileName);
        if (!fs.exists(inFile)) {
            LSHTool.printAndExit("Input file " + fileName + " not found");
        }
        if (!fs.isFile(inFile)) {
            LSHTool.printAndExit("Input " + fileName + " should be a file");
        }

        FSDataInputStream in = fs.open(inFile);
        try {
            this.readFields(in);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            in.close();
        }
    }
}
