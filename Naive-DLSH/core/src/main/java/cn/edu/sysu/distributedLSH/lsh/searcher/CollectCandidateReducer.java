package cn.edu.sysu.distributedLSH.lsh.searcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.sysu.distributedLSH.common.LSHTool;
import cn.edu.sysu.distributedLSH.common.SimpleList;


public class CollectCandidateReducer
extends Reducer<IntWritable, SimpleList, Object, Object> {
    private Configuration conf;
    private FileSystem fs;

    private int radiusID;
    // the number of nearest neighbors that is required to find
    private int kNeighbors;
    private int pruneFactor;
    private int splitNum;
    private int hashTableSize;
    private String baseDir;
    
    private int searchThreshold;
    private int[] splitIndexArr;
    private Vector<Map<Integer, Set<Integer>>> splitCand;
    
    private Set<Integer> emptySet = new HashSet<Integer>();


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

        radiusID = conf.getInt("radiusID", -1);
        kNeighbors = conf.getInt("kNeighbors", 0);
        pruneFactor = conf.getInt("pruneFactor", 0);
        splitNum = conf.getInt("splitNum", 0);
        hashTableSize = conf.getInt("hashTableSize", 0);
        baseDir = conf.get("baseDir");
        
        // the maximum number of real distances to be calculated
        searchThreshold = pruneFactor * hashTableSize + kNeighbors;
        
        try {
            this.getSplitIndex();
        } catch (IOException e) {
            e.printStackTrace();
            LSHTool.printAndExit("get split index failed in CollectCandidateReducer");
        }
        
        splitCand = new Vector<Map<Integer, Set<Integer>>>(splitNum);
        for (int i = 0; i < splitNum; i++) {
            splitCand.add(new HashMap<Integer, Set<Integer>>());
        }
    }
    
    /**
     * get the index of all splits.
     * */
    private void getSplitIndex() throws IOException {
        String dir = baseDir + "/dataset";
        Path path =  new Path(dir);
        FileStatus[] fileStatus = fs.listStatus(path);
        
        splitIndexArr = new int[fileStatus.length];
        for (int i = 0; i < fileStatus.length; i++) {
            splitIndexArr[i] = this.parseStartID(fileStatus[i].getPath().getName());
        }

        Arrays.sort(splitIndexArr);
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
     * reduce.
     * @param key queryID
     * @param values a list of candidate index list
     * @param context
     * */
    @Override
    protected void reduce(final IntWritable key, final Iterable<SimpleList> values,
            final Context context) throws IOException, InterruptedException {
        boolean flag = false;       // break flag
        int count = 0;
        int queryID = key.get();

        for (SimpleList indexList : values) {
            indexList.setCursorToHead();
            while (indexList.hasNext()) {
                count++;
                int candIndex = indexList.next();
                int splitIndex = this.findSplitIndex(candIndex);
                try {
                    // Add the index of the candidate point to the split
                    // in which it is contained.
                    Map<Integer, Set<Integer>> splitMap = splitCand.get(splitIndex);
                    Set<Integer> candSet = splitMap.get(queryID);
                    if (candSet == null) {
                        candSet = new HashSet<Integer>();
                        splitMap.put(queryID, candSet);
                    }
                    candSet.add(candIndex);
                } catch (ArrayIndexOutOfBoundsException e) {
                    e.printStackTrace();
                    LSHTool.printAndExit("splitIndex error for key: " + candIndex);
                }
                if (count >= searchThreshold) {
                    flag = true;
                    break;
                }
            }
            if (flag) {
                break;
            }
        }

        if (0 == count) {
            // There is no points colliding with the query in all data set
            // splits within current search radius. We have to add the query ID
            // to a split, say the first split, with an empty set so that the
            // query will be saved in the intermediate directory later. The
            // reason is the same with why we use an empty list in
            // CollectCandidateMapper.
            splitCand.get(0).put(queryID, emptySet);
        }
    }
    
    /**
     * Find the index of the split in which the candIndex is contained.
     * @param candIndex the index of a data point
     * */
    private int findSplitIndex(final int candIndex) {
        int cursor = -1;
        
        cursor = Arrays.binarySearch(splitIndexArr, candIndex);
        if (cursor < 0) {
            // We can not find candIndex in the array.
            // Now cursor is the insertion point: the index of the first
            // element greater than the key. For more information about
            // insertion point, please refer to the Java document on Arrays.
            return -cursor-2;
        }
        return cursor;
    }
    
    /**
     * cleanup.
     * Save the splitCandMap to hdfs.
     * @param context
     * */
    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
        String file = baseDir + "/splitCand/radius_" + radiusID;
        fs.delete(new Path(file), true);

        int cursor = 0;
        for (Map<Integer, Set<Integer>> splitMap : splitCand) {
            // save to startID.cand
            String outFile = file + "/" + splitIndexArr[cursor] + ".cand";
            Path outPath = new Path(outFile);
            FSDataOutputStream out = fs.create(outPath);
            
            out.writeInt(splitMap.size());
            for (Map.Entry<Integer, Set<Integer>> entry : splitMap.entrySet()) {
                // save the query ID
                out.writeInt(entry.getKey());
                // save the size of the candidate set
                out.writeInt(entry.getValue().size());
                for (int candIndex : entry.getValue()) {
                    out.writeInt(candIndex);
                }
            }
            out.close();
            cursor++;
        }
    }
}
