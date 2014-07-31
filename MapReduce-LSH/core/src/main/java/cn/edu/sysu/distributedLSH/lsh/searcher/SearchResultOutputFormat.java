package cn.edu.sysu.distributedLSH.lsh.searcher;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.edu.sysu.distributedLSH.common.CandidatePriorityQueue;
import cn.edu.sysu.distributedLSH.common.LSHTool;


public class SearchResultOutputFormat<K, V> extends FileOutputFormat<K, V> {
    /**
     * the RecordWriter for search results
     * */
    protected static class SearchResultRecordWriter<K, V> extends RecordWriter<K, V> {
        private static final String utf8 = "UTF-8";
        private DataOutputStream out;

        /**
         * Constructor.
         * */
        public SearchResultRecordWriter(DataOutputStream out) throws IOException {
            this.out = out;
        }

        /**
         * close.
         * */
        public void close(TaskAttemptContext job) throws IOException, InterruptedException {
            out.close();
        }

        /**
         * write.
         * @param key 
         * @param value contains the search result
         * */
        public void write(K key, V value) throws IOException, InterruptedException {
            if (key == null || !(key instanceof IntWritable) || value == null
                    || !(value instanceof CandidatePriorityQueue)) {
                return;
            }
            IntWritable queryIDWritable = (IntWritable)key;
            CandidatePriorityQueue candPriQueue = (CandidatePriorityQueue)value;
            
            int queryID = queryIDWritable.get();
            String str = queryID + " " + candPriQueue.size() + "\n";
            try {
                out.write(str.toString().getBytes(utf8));
                out.write(value.toString().getBytes(utf8));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                LSHTool.printAndExit("Encoding UTF-8 is unsupported");
            }
        }
    }

    /**
     * Implement the abstract method in FileOutputFormat.
     * */
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
        throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        Path filePath = getDefaultWorkFile(job, "");
        FileSystem fs = filePath.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(filePath, false);
        return new SearchResultRecordWriter<K, V>(fileOut);
    }
}
