package partionbycolumn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Manthan Thakker
 * @project HW5
 * @date 3/28/18
 * @email thakker.m@husky.neu.edu
 */
public class MatrixMultiplicationMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {
        //id\t inlink/node/PR#[id1,id2] => {id,inlink/node/PR#[id1,id2]}
        String fields[] = record.toString().split("\t");

        //(inlink/node/PR)#[id1,id2]=>{nodeType,arrayOfIds}
        String valueFields[] = fields[1].split("#");
        String type = valueFields[0];
        if (type.equals("PR")) {

            Text j = new Text();
            Text jBeta = new Text();
            // set the key here
            j.set(fields[0]);
            Double pageRank = Double.parseDouble(valueFields[1]);
            jBeta.set("JBETA#"+j + "#" + pageRank);
            // j beta
            context.write(j, jBeta);

        } else if (type.equals("inlink")) {
         //   System.out.println("valueFields"+ valueFields[1]);
            //idcjs<=2490750#108, 3518302#98, 2459038#23, 1595933#39, 2004213#25, 30062376#456, 226526284#501, 221403927#506
            String idsCjs[] = valueFields[1].replaceAll("\\[","").replaceAll("\\]","").trim().split(", ");
            Text j = new Text();
            Text ijaplha = new Text();
            for (String idCj : idsCjs) {
                String fieldsidCj[]=idCj.split("~");
                if(fieldsidCj.length!=2) {
                    System.out.println("Not 2 "+idCj);
                    return;
                }

                String id=fieldsidCj[0];
                String CjOrAlpha=fieldsidCj[1];
                j.set(id);
                ijaplha.set("IJALPHA#"+id + "," + j + "," + CjOrAlpha);
                context.write(j, ijaplha);
            }

        } else if (type.equals("node")) {
            Text keyNode=new Text();
            keyNode.set(fields[0]);
            Text valueNode=new Text();
            valueNode.set(fields[1]);
            context.write(keyNode,valueNode);
        } else {
            System.out.println("Something is wrong");
        }

        // Default copy for next iteration
        Text keyText=new Text();
        keyText.set(fields[0]);
        Text keyValue=new Text();
        keyValue.set(fields[1]);

        context.write(keyText,keyValue);
    }
}
