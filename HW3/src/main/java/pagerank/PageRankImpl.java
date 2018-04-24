package pagerank;

import enums.PageRankEnums;
import model.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author Manthan Thakker
 * @project HW3
 * @date 2/20/18
 * @email thakker.m@husky.neu.edu
 */
public class PageRankImpl {


    public static void main(String[] args) throws Exception {

        long deltaNew = 0l;


        for (int i = 1; i < 11; i++) {

            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "Page Rank");


            job.setJarByClass(PageRankImpl.class);
            // SETTING CONTEXT VARIABLES
            job.getConfiguration().set("deltaOld", deltaNew + "");
            job.getConfiguration().set("UNIQUEPAGES", args[2]);


            // Mapper
            job.setMapperClass(PageRankMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Node.class);
            job.setReducerClass(PageRankReducer.class);

            //Reducer
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0] + "/" + (i - 1) ));
            FileOutputFormat.setOutputPath(job, new Path(args[0] + "/" + (i) ));
            job.waitForCompletion(true);

            //Getting the number of nodes
            job.getConfiguration().setLong("numberOfNodes", 18000);
            job.getConfiguration().setBoolean("iterate", true);

            // Assigning the dangling node value to old value to use in the next iteration
            deltaNew = job.getCounters().findCounter(PageRankEnums.DANGLINGNODESNEW).getValue();
            // initializiong the new delata dangling node to 0
            job.getCounters().findCounter(PageRankEnums.DANGLINGNODESNEW).setValue(0l);

        }
    }

}

