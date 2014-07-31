package cn.edu.sysu.distributedLSH.lsh.builder;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.sysu.distributedLSH.common.HashTableBlock;
import cn.edu.sysu.distributedLSH.common.IntPair;
import cn.edu.sysu.distributedLSH.common.IntTriple;


public class HashReducer extends Reducer<IntPair, IntTriple, Object, Object> {
    private Configuration conf;
    private FileSystem fs;
    private String baseDir;
    
    private int[] blockSizeArr = null;


    /**
     * setup.
     * @param context
     * */
    @Override
    protected void setup(final Context context) {
        conf = context.getConfiguration();
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        baseDir = conf.get("baseDir");
        this.readBlockSizeInfo();
    }
    
    /**
     * read the size of all blocks from hdfs.
     */
    private void readBlockSizeInfo() {
        String inFile = baseDir + "/hashParam/blockSize.info";
        Path inPath = new Path(inFile);

        try {
            FSDataInputStream in = fs.open(inPath);
            int length = in.readInt();

            blockSizeArr = new int[length];
            for (int i = 0; i < length; i++) {
                blockSizeArr[i] = in.readInt();
            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * reduce.
     * @param key contains an IntPair, whose fields are:
     *  first: the radius ID
     *  second: the block ID
     * @param values contains a list of IntTriple.
     *  IntTriple contains the following fields:
     *  first: the table ID in a block
     *  second: the bucket ID in a hash table
     *  third: the index of the data point 
     * @param context
     * */
    @Override
    protected void reduce(final IntPair key, final Iterable<IntTriple> values,
            final Context context) throws IOException, InterruptedException {
        int blockID, tableID, bucketID, index;
        
        blockID = key.getSecond();
        HashTableBlock tableBlock = new HashTableBlock(key.getFirst(),
                blockID, blockSizeArr[blockID]);

        for (IntTriple intTriple : values) {
            tableID = intTriple.getFirst();
            bucketID = intTriple.getSecond();
            index = intTriple.getThird();
            tableBlock.add(tableID, bucketID, index);
        }

        try {
            tableBlock.saveToHdfs(baseDir, fs);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
