package topk;

import enums.PageRankEnums;
import model.Node;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * @author Manthan Thakker
 * @project HW3
 * @date 2/23/18
 * @email thakker.m@husky.neu.edu
 */
public class TopKMapper extends Mapper<LongWritable, Text, NullWritable, Node> {

    private Map<String, Node> pages;
    private long topK;


    /**
     * Iniitializes all tha variables
     * @param context
     */
    public void setup(Context context) {
        pages = new HashMap<String, Node>();
        this.topK = Long.parseLong(context.getConfiguration().get("K"));
    }


    /**
     *
     * @param key: The Index of the Line
     * @param value: The Node string representation.
     * @param context
     * Emits each node.
     */
    public void map(LongWritable key, Text value, Context context) {
        Node node = parseRecord(value.toString());
        pages.put(node.id, node);
    }


    /**
     * Sorts locally all the collected papges and emits only top k results
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void cleanup(Context context) throws IOException, InterruptedException {
        pages = sortByComparator(pages, false);
        int i = 0;
        for (String page : pages.keySet()) {

            context.write( NullWritable.get(), pages.get(page));
            i++;
            if (i > topK)
                break;
        }
    }


    /**
     *
     * @param record: String representation of the node
     * @return Node object of the given String representation
     */
    public static Node parseRecord(String record) {
        Node node = new Node();
        String fields[] = record.toString().split("#");


        node.id = fields[0].toString().trim();
        node.pageRank = Double.parseDouble(fields[1]);
        String neighborsArr[] = fields[2].substring(1, fields[2].length() - 1).split(",");

        node.neighbors = Arrays.asList(neighborsArr);
        node.isNode = true;

        return node;
    }

    /**
     * Sorts the given Unsorted Map by PageRank Values
     * @param unsortMap : The map to be sorted
     * @param order: False to be ascending
     * @return Sorted Map Order
     * Picked up sorting from the IR Project done in last semester.
     */
    private static Map<String, Node> sortByComparator(Map<String, Node> unsortMap, final boolean order) {

        List<Map.Entry<String, Node>> list = new LinkedList<Map.Entry<String, Node>>(unsortMap.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Map.Entry<String, Node>>() {
            public int compare(Map.Entry<String, Node> o1,
                               Map.Entry<String, Node> o2) {
                if (order) {
                    return o1.getValue().pageRank.compareTo(o2.getValue().pageRank);
                } else {
                    return o2.getValue().pageRank.compareTo(o1.getValue().pageRank);

                }
            }
        });

        // Maintaining insertion order with the help of LinkedList
        Map<String, Node> sortedMap = new LinkedHashMap<String, Node>();
        for (Map.Entry<String, Node> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
}