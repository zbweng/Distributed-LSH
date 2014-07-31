package cn.edu.sysu.distributedLSH.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;


public class DatasetSplit implements Writable {
    private int dimension = -1;
    private int[][] split = null;
    private List<Vector<Integer>> splitBuffer = new LinkedList<Vector<Integer>>();

    /**
     * Constructor.
     * */
    public DatasetSplit() {}
    
    /**
     * get.
     * */
    public int[][] get() {
        return split;
    }
    
    /**
     * set dimension.
     * */
    public void setDimension(final int dimension) {
        this.dimension = dimension;
    }
    
    public void addDataPoint(final int[] point) {
        if (point.length != dimension) {
            LSHTool.printAndExit("array length error in DatasetSplit");
        }

        Vector<Integer> vec = new Vector<Integer>(dimension);
        for (int i = 0; i < dimension; i++) {
            vec.add(point[i]);
        }
        splitBuffer.add(vec);
    }
    
    /**
     * Implement the method in the interface Writable.
     * @param out output stream
     * */
    public void write(final DataOutput out) throws IOException {
        int length = splitBuffer.size();

        out.writeInt(length);
        out.writeInt(dimension);
        for (Vector<Integer> vec : splitBuffer) {
            for (int i = 0; i < dimension; i++) {
                out.writeInt(vec.get(i));
            }
        }
    }

    /**
     * Implement the method in the interface Writable.
     * @param in input stream
     * */
    public void readFields(final DataInput in) throws IOException {
        int length = in.readInt();

        dimension = in.readInt();
        split = new int[length][];
        for (int i = 0; i < length; i++) {
            split[i] = new int[dimension];
            for (int j = 0; j < dimension; j++) {
                split[i][j] = in.readInt();
            }
        }
    }

    /**
     * Save the DatasetSplit to hdfs.
     * @param baseDir the base directory
     * @param fs
     * @param startID the start ID of this data set split
     * */
    public void saveToHdfs(final String baseDir, final FileSystem fs, final int startID)
            throws IOException {
        String fileName = baseDir + "/dataset/" + startID + ".split";

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
     * Read the DatasetSplit from hdfs.
     * @param radiusID the radius ID of the LSH
     * @param baseDir the base directory
     * @param fs
     * @param startID the start ID of this data set split
     * */
    public void readFromHdfs(final String baseDir, final FileSystem fs, final int startID)
            throws IOException {
        String fileName = baseDir + "/dataset/" + startID + ".split";

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
