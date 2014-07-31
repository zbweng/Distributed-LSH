package cn.edu.sysu.distributedLSH.common;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HashTable extends BucketTable {
    private int radiusID;
    private int tableID;

    /**
     * Empty constructor.
     * */
    public HashTable() {}

    /**
     * Constructor.
     * */
    public HashTable(final int radiusID, final int tableID) {
        this.radiusID = radiusID;
        this.tableID = tableID;
    }

    /**
     * hashCode.
     * */
    @Override
    public int hashCode() {
        return radiusID * 797 + tableID;
    }

    /**
     * equals.
     * */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HashTable) {
            HashTable that = (HashTable)obj;
            if (this.tableID == that.tableID && this.radiusID == that.radiusID) {
                return this.bucketTable.equals(that.bucketTable);
            }
        }
        return false;
    }

    /**
     * @param dir the directory
     * @param conf
     * */
    public void saveToHdfs(final String dir, final FileSystem fs)
            throws IOException {
        String fileName = dir + "/radius_" + radiusID + "/" + tableID + ".table";

        Path outFile = new Path(fileName);
        if (fs.exists(outFile)) {
            LSHTool.printAndExit("Output file " + fileName + " already exists");
        }

        FSDataOutputStream out = fs.create(outFile);
        try {
            super.write(out);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            out.close();
        }
    }

    /**
     * @param dir the directory
     * @param conf
     * */
    public void readFromHdfs(final String dir, final FileSystem fs)
            throws IOException {
        String fileName = dir + "/radius_" + radiusID + "/" + tableID + ".table";

        Path inFile = new Path(fileName);
        if (!fs.exists(inFile)) {
            LSHTool.printAndExit("Input file " + fileName + " not found");
        }
        if (!fs.isFile(inFile)) {
            LSHTool.printAndExit("Input " + fileName + " should be a file");
        }

        FSDataInputStream in = fs.open(inFile);
        try {
            super.readFields(in);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            in.close();
        }
    }
}
