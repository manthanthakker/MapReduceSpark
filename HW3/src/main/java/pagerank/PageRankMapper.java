package pagerank;

import enums.PageRankEnums;
import model.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Manthan Thakker
 * @project HW3
 * @date 2/23/18
 * @email thakker.m@husky.neu.edu
 */

public class PageRankMapper extends Mapper<Object, Text, Text, Node> {

    double deltaOld = 0.0;
    Long numberOfNodes;
    Configuration configuration;
    private long SCALE_FACTOR = 10000000000000000l;


    /**
     * Initializes the variables from context
     * @param context
     */
    public void setup(Context context) {

        configuration = context.getConfiguration();
        deltaOld = Long.parseLong(context.getConfiguration().get("deltaOld"))*1.0 / (SCALE_FACTOR);
        numberOfNodes = Long.parseLong(context.getConfiguration().get("UNIQUEPAGES"));
    }


    /**
     *
     * @param nodeId: The Id of the Node
     * @param record: the record which contains the string representation of the node
     * @param context
     * @throws IOException
     * @throws InterruptedException
     *
     * This methods takes in Value as the Node String representation, converts it into object
     * It emits a copy of the same to the map reduce phase and then emits the page rank contribution for each
     * of the neighbor nodes.
     */
    public void map(Object nodeId, Text record, Context context) throws IOException, InterruptedException {

        /// Node in String Format Converted to the Node Object
        Node node = parseRecord(record.toString(), numberOfNodes);

        // Add contribution from dangling nodes to PageRank
        node.pageRank += 0.85 * (deltaOld / numberOfNodes);

        // Pass along the graph
        context.write(new Text(node.id.trim()), node);

        // Emit the pageRank contribution to the neighboring nodes
        Double p = 0.0;
        if (node.neighbors.size() > 1) {
            p = node.pageRank / (numberOfNodes);

            // Contribute Partial PageRank for each of its neighbor
            for (String n : node.neighbors) {

                // Node with just pageRank, isNode Field is set to be false
                Node pageRankNode = new Node(n, p);
                pageRankNode.id = n.trim();
                context.write(new Text(n.trim()), pageRankNode);

            }
        } else {

            // If a node has no neighbours than add to the dangling nodes new
            double pageRank = ((node.pageRank / (numberOfNodes)));
            context.getCounter(PageRankEnums.DANGLINGNODESNEW).increment((long) (pageRank * SCALE_FACTOR));
        }

    }

    /**
     *
     * @param record: String representation of the node
     * @param numberOfNodes: The unique page Count
     * @return Node object of the given String representation
     */
    public static Node parseRecord(String record, long numberOfNodes) {
        Node node = new Node();
        String fields[] = record.toString().split("#");

        node.id = fields[0].toString().trim();
        if (node.pageRank != -1.0)
            node.pageRank = Double.parseDouble(fields[1]);
        else
            node.pageRank = 1.0 / numberOfNodes;
        String neighborsArr[] = fields[2].substring(1, fields[2].length() - 1).split(",");
        node.neighbors = Arrays.asList(neighborsArr);
        node.isNode = true;
        return node;
    }

}

