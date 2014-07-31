package cn.edu.sysu.distributedLSH;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;

import cn.edu.sysu.distributedLSH.lsh.builder.HashTableBuilder;
import cn.edu.sysu.distributedLSH.lsh.searcher.LSHSearcher;
import cn.edu.sysu.distributedLSH.statistics.Statistician;


public class DistributedLSH extends Configured implements Tool {
    private Configuration conf;
    private FileSystem fs;


    /**
     * Constructor.
     * */
    public DistributedLSH() {
        conf = null;
        fs = null;
    }

    /**
     * Print usage.
     * */
    private void printUsage() {
        System.out.print("Usage:\n");
        System.out.print("  -b,\t\t\tbuild hash tables\n");
        System.out.print("  -s,\t\t\tsearch near neighbors\n");
        System.out.print("  -bs,\t\t\tbuild hash talbles then search\n");
    }

    /**
     * Run from ToolRunner.
     * @param args the arguments
     * @return 0 if successful
     * */
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        conf = getConf();

        String resourceName = "conf/MRLSH-site." + conf.get("dataset");
        conf.addResource(resourceName);

        fs = FileSystem.get(conf);

        boolean isBuild = false;
        boolean isSearch = false;

        // Parse parameters.
        if (1 == args.length) {
            if (args[0].equals("-b")) {
                isBuild = true;
            } else if (args[0].equals("-s")) {
                isSearch = true;
            } else if (args[0].equals("-bs")) {
                isBuild = true;
                isSearch = true;
            } else {
                printUsage();
                return 0;
            }
        } else {
            printUsage();
            return 0;
        }

        if (isBuild) {
            // get some statistics
            Statistician statistician = new Statistician(conf, fs);
            statistician.run();

            // build hash tables
            HashTableBuilder hashTableBuilder = new HashTableBuilder(conf, fs);
            hashTableBuilder.run();
        }

        if (isSearch) {
            LSHSearcher lshSearcher = new LSHSearcher(conf, fs);
            lshSearcher.run();
        }

        return 0;
    }

    /**
     * The main method of distributedLSH.
     * */
    public static void main(final String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DistributedLSH(), args);
        System.exit(exitCode);
    }
}
