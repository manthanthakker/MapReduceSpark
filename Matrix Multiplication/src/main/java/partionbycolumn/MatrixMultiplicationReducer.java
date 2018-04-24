package partionbycolumn;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * @author Manthan Thakker
 * @project HW5
 * @date 3/28/18
 * @email thakker.m@husky.neu.edu
 */
public class MatrixMultiplicationReducer extends Reducer<Text, Text, Text, Text> {

    /**
     * Types of nodes and format:
     * <p>
     * "JBETA#"+j + "#" + pageRank
     * "IJALPHA#"+id + "," + j + "," + CjOrAlpha
     * "inlink#"+outLinks+""
     * "PR#"+0.0
     * "node# "+page[2]+" "+page[1]+" "+page[2]+" "+page[3]+" "+page[4]
     */
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        HashMap<Long, Double> jBeta = new HashMap<Long, Double>();
        HashMap<Long, Long> IJALPHA = new HashMap<Long, Long>();
        Double pageRank;
        Double temp = 0.0;
        while (iterator.hasNext()) {
            String nodeFields[] = iterator.next().toString().split("#");
            String nodeType = nodeFields[0];
            System.out.println("Type of node in reducer is " + nodeType);
            if (nodeType.equals("JBETA")) {
                long j = Long.parseLong(nodeFields[1]);
                Double aplha = Double.parseDouble(nodeFields[2]);
                jBeta.put(j, aplha);
            } else if (nodeType.equals("IJALPHA")) {
                System.out.println("IJALpha node has :" + nodeFields[1]);
                String fields[] = nodeFields[1].split(",");
                IJALPHA.put(Long.parseLong(fields[1]), Long.parseLong(fields[2]));
            } else if (nodeType.equals("PR")) {
                pageRank = Double.parseDouble(nodeFields[1]);
            } else if (nodeType.equals("node")) {
                // dont know
            }
        }
        for (Long j : jBeta.keySet()) {
            System.out.println("J value is " + IJALPHA.getOrDefault(j, 0l) + " " + jBeta.getOrDefault(j, 0.0) + "\n" + IJALPHA + "\n" + jBeta);
            temp = temp + IJALPHA.getOrDefault(j, 0l) * 1.0;//jBeta.getOrDefault(j, 1.0);
        }
        Text pageRankValue = new Text();
        pageRankValue.set(temp + "");
        context.write(pageRankValue, key);
    }


}
