package cn.edu.sysu.distributedLSH.lsh.builder;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.sysu.distributedLSH.common.HashTable;
import cn.edu.sysu.distributedLSH.common.LSH;
import cn.edu.sysu.distributedLSH.common.LSHTool;
import cn.edu.sysu.distributedLSH.common.TwoDArray;


public class HashReducer extends Reducer<IntWritable, Text, Object, Object> {
    private static final int THRESHOLD_RADIUS = 1;

    private Configuration conf;
    private FileSystem fs;

    private int ratio;
    private String baseDir;

    // statistics
    private int dimension = -1;
    private int maxCoordinate = -1;
    private int nRadii = -1;

    private boolean flag = true;
    private int partDataSetSize = 0;        // the size of this part of data set
    private String partDir;                 // base directory for this partition
    private int[] radii;
    private LSH lsh = null;
    private int hashTableSize;
    private TwoDArray dataPoints = null;    // data points
    private List<String> valuesBuffer = null;


    /**
     * setup.
     * 
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

        ratio = conf.getInt("ratio", 0);
        baseDir = conf.get("baseDir");

        this.readStatistics();
        
        // initialize multiple radii
        radii = new int[nRadii];
        radii[0] = THRESHOLD_RADIUS;
        for (int i = 1; i < nRadii; i++) {
            radii[i] = ratio * radii[i - 1];
        }
    }

    /**
     * read the final statistics file from hdfs.
     * */
    private void readStatistics() {
        String statFile = baseDir + "/stat/final.stat";
        Path statPath = new Path(statFile);

        try {
            if (!fs.isFile(statPath)) {
                LSHTool.printAndExit("statistics file does not exist");
            }
            FSDataInputStream in = fs.open(statPath);

            // The following variables are output to hdfs by Statistician.
            dimension = in.readInt();
            maxCoordinate = in.readInt();
            nRadii = in.readInt();
            in.close();

            if (dimension < 1 || maxCoordinate < 1 || nRadii < 1) {
                LSHTool.printAndExit("statistics error");
            }
        } catch (IOException e) {
            e.printStackTrace();
            LSHTool.printAndExit("read statistics file error");
        }
    }

    /**
     * reduce.
     * @param key contains the partID
     * @param values contains a list of data points.
     * @param context
     * */
    @Override
    protected void reduce(final IntWritable key, final Iterable<Text> values,
            final Context context) throws IOException, InterruptedException {
        if (flag) {
            flag = false;
        } else {
            LSHTool.printAndExit("HashReducer reduce() is called more than once");
        }

        partDataSetSize = convertValues(values);

        partDir = baseDir + "/part_" + key.get();
        // We must delete all built LSHs and hash tables in this partition since
        fs.delete(new Path(partDir), true);

        this.buildLsh();
        hashTableSize = lsh.getHashTableSize();
        dataPoints = new TwoDArray(partDataSetSize, dimension);

        this.parseDataSet();
        this.hash();
    }

    /**
     * Convert values into a list of String then store it in memory.
     * @param values contains a list of data points
     * @return the size of this partition of the data set
     * */
    private int convertValues(final Iterable<Text> values) {
        int size = 0;

        valuesBuffer = new LinkedList<String>();
        Iterator<Text> it = values.iterator();
        while (it.hasNext()) {
            valuesBuffer.add(new String(it.next().toString()));
            size++;
        }
        return size;
    }

    /**
     * Build a LSH instance then save it to hdfs. Here, we use the same LSH
     * for multiple radii.
     * */
    private void buildLsh() throws IOException {
        lsh = new LSH(dimension);
        lsh.calcParameters(maxCoordinate, partDataSetSize, ratio);
        lsh.saveToHdfs(partDir, fs);
    }

    /**
     * parse this part of the data set which is stored in valuesBuffer
     * @throws IOException 
     * */
    private void parseDataSet() throws IOException {
        int[][] points = dataPoints.get();

        int index = 0;
        for (String str : valuesBuffer) {
            Scanner scanner = null;
            try {
                scanner = new Scanner(str);
                // skip the line number
                scanner.nextInt();
                for (int i = 0; i < dimension; i++) {
                    points[index][i] = scanner.nextInt();
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                e.printStackTrace();
            } finally {
                scanner.close();
            }
            index++;
        }

        dataPoints.saveToHdfs(partDir, fs);
    }

    /**
     * Hash data points to hash tables.
     * */
    private void hash() {
        int[][] points = dataPoints.get();

        for (int i = 0; i < nRadii; i++) {
            // i is the radius id 
            for (int j = 0; j < hashTableSize; j++) {
                // j is the table id
                HashTable hashTable = new HashTable(i, j);
                for (int index = 0; index < partDataSetSize; index++) {
                    int bucketID = lsh.calcHashValue(j, radii[i], points[index]);
                    hashTable.add(bucketID, index);
                }
                // save hash table to hdfs
                try {
                    hashTable.saveToHdfs(partDir, fs);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
