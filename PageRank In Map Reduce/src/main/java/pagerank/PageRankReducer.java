package pagerank;

import enums.PageRankEnums;
import model.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Manthan Thakker
 * @project HW3
 * @date 2/23/18
 * @email thakker.m@husky.neu.edu
 */
public class PageRankReducer extends Reducer<Text, Node, Text, Text> {

    Long numberOfNodes;
    Configuration configuration;
    private final long SCALE_FACTOR = 1000000000000l;

    /**
     * Initializes the state variables
     * @param context
     */
    public void setup(Context context) {
        configuration = context.getConfiguration();
        numberOfNodes = Long.parseLong(context.getConfiguration().get("UNIQUEPAGES"));
    }

    /**
     *
     * @param key: The Node id
     * @param values: List of Nodes/Pagerank Contributions(isNode will be false)
     * @param context: Context
     * @throws IOException
     * @throws InterruptedException
     * All partial pagerank contributions for same nodeId will be routed to the same reduce call.
     * A copy of the Node itself will be routed to the same reduce call.
     * We add up all the partial contributions and then emit the new node with the updated pageRank.
     */
    public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {

        // Intializing variables:
        Double pageRankTotal = 0.0;
        Iterator<Node> iterable = values.iterator();
        Node M = null;

        // Preparing the string to be outputed
        String MString = "";

        while (iterable.hasNext()) {
            Node node = iterable.next();
            // If its a pagerank contribution or Actual Node.
            if (node.isNode) {
                M = node;
                MString = "#" + M.neighbors + "#" + M.isNode;
            } else {
                pageRankTotal += node.pageRank;
            }
        }

        // The pagerank formula
        pageRankTotal = (0.15 / (numberOfNodes)) + (0.85 * pageRankTotal);

        if (M != null) {
            context.write(new Text(M.id.trim()), new Text("#" + pageRankTotal + MString));
        } else {
            long pageRank = Double.valueOf(pageRankTotal * SCALE_FACTOR).longValue();
            context.getCounter(PageRankEnums.DANGLINGNODESNEW).increment(pageRank);
            context.write(key, new Text("#" + pageRankTotal + "#[]#true"));
        }
    }


}

