package filterpagerankvector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


/**
 * @author Manthan Thakker
 * @project HW5
 * @date 3/29/18
 * @email thakker.m@husky.neu.edu
 */

/**
 * "PR#"+0.0
 */
public class PageRankVectorMapper extends Mapper<LongWritable, Text, Text, Text> {





    public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {

        String fields[] = record.toString().split("\t");
        //(inlink/node/PR)#[id1,id2]=>{nodeType,arrayOfIds}
        String valueFields[] = fields[1].split("#");
        String type = valueFields[0];
        if (type.equals("PR")) {
            Text id = new Text();
            id.set(fields[0]);
            Text value = new Text();
            // TODO: replace by Unique Pagees
            value.set(1.0 / 26000.0 + "");
            context.write(id, value);
        }
    }



}
