package cn.edu.sysu.distributedLSH.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;


/**
 * BucketTable can be used to store a part of a hash table in memory.
 * */
public class BucketTable implements Writable {
    // key: bucket ID
    // value: the IDs of the data points (in data set or query set) that are hashed to this bucket
    protected Map<Integer, SimpleList> bucketTable;


    /**
     * Constructor.
     * */
    public BucketTable() {
        bucketTable = new HashMap<Integer, SimpleList>();
    }

    /**
     * Add the index into the bucket.
     * @param bucketID bucket ID
     * @param index index
     * */
    public void add(final int bucketID, final int index) {
        SimpleList bucket = bucketTable.get(bucketID);
        if (null == bucket) {
            bucket = new SimpleList();
            bucketTable.put(bucketID, bucket);
        }
        bucket.add(index);
    }

    /**
     * Get the bucket whose ID is bucketID.
     * @param bucketID bucket ID
     * */
    public SimpleList getBucket(final int bucketID) {
        return bucketTable.get(bucketID);
    }

    /**
     * clear.
     * */
    public void clear() {
        bucketTable.clear();
    }

    /**
     * hashCode.
     * */
    @Override
    public int hashCode() {
        return bucketTable.hashCode();
    }

    /**
     * equals.
     * */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BucketTable) {
            BucketTable that = (BucketTable)obj;
            return this.bucketTable.equals(that.bucketTable);
        }
        return false;
    }

    /**
     * Implement the method in the interface Writable.
     * @param out output stream
     * */
    public void write(final DataOutput out) throws IOException {
        out.writeInt(bucketTable.size());
        for (Map.Entry<Integer, SimpleList> entry : bucketTable.entrySet()) {
            out.writeInt(entry.getKey());
            entry.getValue().write(out);
        }
    }

    /**
     * Implement the method in the interface Writable.
     * @param in input stream
     * */
    public void readFields(final DataInput in) throws IOException {
        // The clear operation is indispensable since the RecordReader for a
        // specific input format always reuses its key and value instance for
        // the sake of efficiency. If we don't clear the Map before we read,
        // the Map will get more and more items, which is not the result
        // we want.
        this.clear();

        int tableSize = in.readInt();
        for (int i = 0; i < tableSize; i++) {
            int bucketID = in.readInt();
            SimpleList queryList = new SimpleList();
            queryList.readFields(in);
            bucketTable.put(bucketID, queryList);
        }
    }
}
