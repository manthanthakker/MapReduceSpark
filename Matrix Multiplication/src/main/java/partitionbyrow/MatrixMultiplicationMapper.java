package partitionbyrow;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.*;
import java.util.HashMap;

/**
 * @author Manthan Thakker
 * @project HW5
 * @date 3/28/18
 * @email thakker.m@husky.neu.edu
 */
public class MatrixMultiplicationMapper extends Mapper<LongWritable, Text, Text, Text> {

    HashMap<Long, Double> idPageRank;


    public void setup(Context context) {
        idPageRank = new HashMap<>();
        populateCache(context);
    }

    void populateCache(Context context) {
        BufferedReader reader=null;
        try {
            Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            System.out.println(stopWordsFiles);

            // Direct access by name
            File baz = new File("cache/cache");

            if (stopWordsFiles != null && stopWordsFiles.length > 0) {

                reader = new BufferedReader(new FileReader(baz));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length == 2) {
                        Long key = Long.parseLong(parts[0]);
                        Double value = Double.parseDouble(parts[1]);
                        idPageRank.put(key, value);
                    } else {
                        //  System.out.println("ignoring line: " + line);
                    }
                }
                reader.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error in reading cache" + e);
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * Types of nodes and format:
     * <p>
     * <p>
     * "inlink#"+outLinks+""
     * "PR#"+0.0
     * "node# "+page[2]+" "+page[1]+" "+page[2]+" "+page[3]+" "+page[4]
     */
    public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {
        //id\t inlink/node/PR#[id1,id2] => {id,inlink/node/PR#[id1,id2]}
        String fields[] = record.toString().split("\t");

        //(inlink/node/PR)#[id1,id2]=>{nodeType,arrayOfIds}
        String valueFields[] = fields[1].split("#");
        String type = valueFields[0];
        if (type.equals("inlink")) {

            double temp = 0.0;

            //idcjs<=2490750#108, 3518302#98, 2459038#23, 1595933#39, 2004213#25, 30062376#456, 226526284#501, 221403927#506
            String idsCjs[] = valueFields[1].replaceAll("\\[", "").replaceAll("\\]", "").trim().split(", ");


            for (String idCj : idsCjs) {
                String fieldsidCj[] = idCj.split("~");
                if (fieldsidCj.length != 2) {
                    return;
                }

                String id = fieldsidCj[0];
                String CjOrAlpha = fieldsidCj[1];
                temp += Double.parseDouble(CjOrAlpha) * idPageRank.getOrDefault(Long.parseLong(id), 0.0);
            }
            Text pageId = new Text();
            pageId.set(fields[0]);
            Text pageRank = new Text();
            pageRank.set(temp + "");

            context.write(pageId, pageRank);

        }

    }
}
