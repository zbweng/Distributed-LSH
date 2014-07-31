package cn.edu.sysu.distributedLSH.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;


public class HashTableBlock  implements Writable {
    private int radiusID;
    private int blockID;
    private int blockSize;
    private BucketTable[] hashTables = null;

    /**
     * Constructor.
     * */
    public HashTableBlock(final int radiusID, final int blockID) {
        this.radiusID = radiusID;
        this.blockID = blockID;
    }

    /**
     * Constructor.
     * */
    public HashTableBlock(final int radiusID, final int blockID, final int blockSize) {
        this.radiusID = radiusID;
        this.blockID = blockID;
        this.blockSize = blockSize;

        hashTables = new BucketTable[blockSize];
        for (int i = 0; i < blockSize; i++) {
            hashTables[i] = new BucketTable();
        }
    }
    
    /**
     * get blockSize.
     * */
    public int getBlockSize() {
        return blockSize;
    }
    
    /**
     * get bucket by block table ID.
     * @param blockTableID block table ID
     * @param bucketID bucket ID
     * */
    public SimpleList getBucket(final int blockTableID, final int bucketID) {
        return hashTables[blockTableID].getBucket(bucketID);
    }
    
    /**
     * Add the index into the bucket of the blockTableID hash table.
     * @param blockTableID block table ID
     * @param bucketID bucket ID
     * @param index index
     * */
    public void add(final int blockTableID, final int bucketID, final int index) {
        hashTables[blockTableID].add(bucketID, index);
    }

    /**
     * hashCode.
     * */
    @Override
    public int hashCode() {
        return radiusID * 797 + blockID;
    }

    /**
     * equals.
     * */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HashTableBlock) {
            HashTableBlock that = (HashTableBlock)obj;
            if (this.radiusID == that.radiusID && this.blockID == that.blockID) {
                return true;
            }
        }
        return false;
    }

    /**
     * Implement the method in the interface Writable.
     * @param out output stream
     * */
    public void write(final DataOutput out) throws IOException {
        out.writeInt(blockSize);
        for (int i = 0; i < blockSize; i++) {
            hashTables[i].write(out);
        }
    }

    /**
     * Implement the method in the interface Writable.
     * @param in input stream
     * */
    public void readFields(final DataInput in) throws IOException {
        blockSize = in.readInt();

        hashTables = new BucketTable[blockSize];
        for (int i = 0; i < blockSize; i++) {
            hashTables[i] = new BucketTable();
            hashTables[i].readFields(in);
        }
    }

    /**
     * @param baseDir the base directory
     * @param fs
     * */
    public void saveToHdfs(final String baseDir, final FileSystem fs) throws IOException {
        String fileName = baseDir + "/radius_" + radiusID + "/" + blockID + ".tableBlock";

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
     * @param baseDir the base directory
     * @param fs
     * */
    public void readFromHdfs(final String baseDir, final FileSystem fs) throws IOException {
        String fileName = baseDir + "/radius_" + radiusID + "/" + blockID + ".tableBlock";

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
