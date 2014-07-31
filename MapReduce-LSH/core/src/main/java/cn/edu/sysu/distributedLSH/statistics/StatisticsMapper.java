package cn.edu.sysu.distributedLSH.statistics;

import java.io.IOException;
import java.lang.Math;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class StatisticsMapper extends Mapper<Object, Text, Object, Object> {
    private Configuration conf;
    private FileSystem fs;

    private String baseDir;
    
    private int dimension;
    private boolean flag = true;
    private int[] point = null;

    // the maximum absolute value of coordinate in the split of the data set
    private int maxCoordinate = -1;


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
    }

    /**
     * map.
     * @param key dummy
     * @param value contains the data point
     * @param context
     * */
    @Override
    protected void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        Scanner scanner = null;
        int curAbs;
        
        if (flag) {
            // The following code will be processed only once by a mapper.
            dimension = getDimension(value.toString());
            point = new int[dimension];
            flag = false;
        }

        try {
            scanner = new Scanner(value.toString());
            // skip the line number
            scanner.nextInt();
            for (int i = 0; i < dimension; i++) {
                point[i] = scanner.nextInt();
                curAbs = Math.abs(point[i]);
                if (curAbs > maxCoordinate) {
                    maxCoordinate = curAbs;
                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            e.printStackTrace();
        } finally {
            scanner.close();
        }
    }
    
    /**
     * Get the dimension of the given data point.
     * @param dataPoint
     * */
    private int getDimension(final String dataPoint) {
        Scanner scanner = null;
        int dim = 0;

        scanner = new Scanner(dataPoint.toString());
        // skip the line number
        scanner.nextInt();
        while (scanner.hasNext()) {
            scanner.nextInt();
            dim++;
        }
        scanner.close();

        return dim;
    }
    
    /**
     * cleanup.
     * @param context
     * */
    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
        String statFile = baseDir + "/stat/split_" + context.getInputSplit().toString().hashCode();
        Path statPath = new Path(statFile);

        fs.delete(statPath, false);
        
        FSDataOutputStream out = fs.create(statPath);
        try {
            out.writeInt(dimension);
            out.writeInt(maxCoordinate);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            out.close();
        }
    }
}
