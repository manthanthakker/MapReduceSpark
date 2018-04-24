package partitionbyrow;

import enums.Counter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Manthan Thakker
 * @project HW5
 * @date 3/28/18
 * @email thakker.m@husky.neu.edu
 */
public class Driver {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {


        for (int i = 0; i < 10; i++) {

            Configuration conf = new Configuration();


            Job job = Job.getInstance(conf, "Matrix multiplication2");


            if (i > 0) {
                File file = new File(args[1]);
                FileUtils.deleteDirectory(file);
            }
            // Setup
            job.setJarByClass(Driver.class);
            job.setMapperClass(MatrixMultiplicationMapper.class);

            // load the cache file
            FileSystem fs1 = FileSystem.get(conf);
            fs1.copyFromLocalFile(new Path(args[2]),
                    new Path("/user/hadoop/dir"));


            DistributedCache.createSymlink(conf);
            DistributedCache.addCacheFile(
                    new URI("hdfs://localhost:9000/user/hadoop/dir/cache#cache"), conf);

            DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());



            //Mapper
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);




            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
            job.getCounters().findCounter(Counter.INDEX).setValue(1000000l);
            FileSystem fs = FileSystem.get(conf);


            // updateCache
            File file = new File(args[2]);
            FileUtils.deleteDirectory(file);
            Path srcPath = new Path(args[1]);
            Path dstPath = new Path(args[2] + "/cache");
            FileUtil.copyMerge(fs, srcPath, fs, dstPath, false, conf, "");
        }
    }
}
