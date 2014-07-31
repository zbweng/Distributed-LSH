package cn.edu.sysu.distributedLSH.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;


/**
 * TwoDArray can be used to store a part of data set.
 * */
public class TwoDArray implements Writable {
    private int row = 0;
    private int col = 0;
    private int[][] array = null;


    /**
     * Default constructor.
     * */
    public TwoDArray() {}

    /**
     * Constructor. new a 2-d array with size row * col
     * @param row
     * @param col 
     * */
    public TwoDArray(final int row, final int col) {
        this.row = row;
        this.col = col;
        array = new int[row][];
        for (int i = 0; i < row; i++) {
            array[i] = new int[col];
        }
    }

    /**
     * get.
     * */
    public int[][] get() {
        return array;
    }

    /**
     * Implement the method in the interface Writable.
     * @param out output stream
     * */
    public void write(final DataOutput out) throws IOException {
        out.writeInt(row);
        out.writeInt(col);
        for (int i = 0; i < row; i++) {
            for (int j = 0; j < col; j++) {
                out.writeInt(array[i][j]);
            }
        }
    }

    /**
     * Implement the method in the interface Writable.
     * @param out output stream
     * */
    public void readFields(final DataInput in) throws IOException {
        row = in.readInt();
        col = in.readInt();
        array = new int[row][];
        for (int i = 0; i < row; i++) {
            array[i] = new int[col];
            for (int j = 0; j < col; j++) {
                array[i][j] = in.readInt();
            }
        }
    }

    /**
     * @param partDir the directory of this partition of the data set
     * @param conf
     * */
    public void saveToHdfs(final String partDir, final FileSystem fs) throws IOException {
        String fileName = partDir + "/part.dataset";

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
     * @param partDir the directory of this partition of the data set
     * @param conf
     * */
    public void readFromHdfs(final String partDir, final FileSystem fs) throws IOException {
        String fileName = partDir + "/part.dataset";

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
