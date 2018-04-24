package topk;

import model.Node;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Manthan Thakker
 * @project HW3
 * @date 2/23/18
 * @email thakker.m@husky.neu.edu
 */
public class TopK {


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top K");
        job.getConfiguration().set("UNIQUEPAGES", args[2]);

        job.getConfiguration().set("K",args[3]);
        // Setup
        job.setJarByClass(TopK.class);
        job.setMapperClass(TopKMapper.class);

        //Mapper
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Node.class);

        job.setReducerClass(TopKReducer.class);
        //Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
