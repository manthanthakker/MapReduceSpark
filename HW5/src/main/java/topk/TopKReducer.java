package topk;

import model.Node;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * @author Manthan Thakker
 * @project HW3
 * @date 2/23/18
 * @email thakker.m@husky.neu.edu
 */
public class TopKReducer extends Reducer<NullWritable, Node, Text, Text> {
    private Map<String, Node> pages;
    private long topK;

    /**
     * Initialzes all the variables
     *
     * @param context
     */
    public void setup(Context context) {
        pages = new HashMap<String, Node>();
        this.topK = Long.parseLong(context.getConfiguration().get("K"));
    }


    /**
     * @param key:          The Index of the Line
     * @param nodeIterator: List of Nodes
     * @param context       As we know all records will route to the same reduce call.
     *                      Inserts each node and then just sorts and emits the top k results.
     */
    public void reduce(NullWritable key, Iterable<Node> nodeIterator, Context context) throws IOException, InterruptedException {
        Iterator<Node> iterator = nodeIterator.iterator();
        while (iterator.hasNext()) {
            Node node = iterator.next();

            pages.put(node.pageRank + "#" + node.id, node);
        }
        pages = sortByComparator(pages, false);
        int i = 0;
        for (String page : pages.keySet()) {
            context.write(new Text(""), new Text(page));
            i++;
            if (i > topK)
                break;
        }
    }

    /**
     * Sorts the given Unsorted Map by PageRank Values
     *
     * @param unsortMap : The map to be sorted
     * @param order:    False to be ascending
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
                    return (int) (((Double.parseDouble(o2.getKey().split("#")[0]) * 10000000000.0) - (Double.parseDouble(o1.getKey().split("#")[0])) * 10000000000.0) * 10000.0);

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