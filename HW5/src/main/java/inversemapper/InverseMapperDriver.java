package inversemapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Manthan Thakker
 * @project HW5
 * @date 3/29/18
 * @email thakker.m@husky.neu.edu
 */
public class InverseMapperDriver {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InverseMapper");

        // Setup
        job.setJarByClass(InverseMapperDriver.class);
        job.setMapperClass(InverseMapper.class);


        FileSystem fs1 = FileSystem.get(conf);
        fs1.copyFromLocalFile(new Path(args[2]),
                new Path("/user/hadoop/dir/"));

        DistributedCache.createSymlink(conf);
        DistributedCache.addCacheFile(
                new URI("hdfs://localhost:9000/user/hadoop/dir/mapper#part-r-00000"), conf);

        //Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
