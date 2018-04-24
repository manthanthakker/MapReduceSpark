package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;


/**
 * @author Manthan Thakker
 * @project HW5
 * @date 3/29/18
 * @email thakker.m@husky.neu.edu
 */

/**
 * "PR#"+0.0
 */
public class PrepareMapperFile extends Mapper<LongWritable, Text, Text, Text> {





    public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {

        String fields[] = record.toString().split("\t");

        //(inlink/node/PR)#[id1,id2]=>{nodeType,arrayOfIds}
        String valueFields[] = fields[1].split("#");
        String type = valueFields[0];

        if (type.equals("node")) {
            String nodeString[] = valueFields[1].trim().split(" ");
            Text id = new Text();
            id.set(nodeString[0]);
            Text pageName = new Text();
            pageName.set(nodeString[1]);
            context.write(id,pageName);
        }

    }


}
