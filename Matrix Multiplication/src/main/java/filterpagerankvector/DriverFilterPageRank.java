package filterpagerankvector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Manthan Thakker
 * @project HW5
 * @date 3/29/18
 * @email thakker.m@husky.neu.edu
 */
public class DriverFilterPageRank {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PrepareCache");

        // Setup
        job.setJarByClass(DriverFilterPageRank.class);
        job.setMapperClass(PageRankVectorMapper.class);

        //Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(args[1]);
        Path dstPath = new Path(args[2] + "/cache");

        FileUtil.copyMerge(fs, srcPath, fs, dstPath, false, conf, "");
    }
}
