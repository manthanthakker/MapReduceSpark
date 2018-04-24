package parserjob;

import enums.Counter;
import enums.PageRankEnums;
import model.Page;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Manthan Thakker
 * @project HW5
 * @date 3/26/18
 * @email thakker.m@husky.neu.edu
 */

/**
 *
 * Preporcessing
 * Parses each file
 * and emits 4 types of nodes
 * 1. Node with unique Id key (#node)
 * 2. Node with  its inlinks and cj (#inlink)
 * 3. PageRank Node (#PR)
 */
public class ParserReducer extends Reducer<Text, Text, Text, Text> {

    String thePage = null;
    List<String> outLinks;
    Text inLinkPage;


    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // increment UNIQUE PAGES
        context.getCounter(PageRankEnums.UNIQUEPAGES).increment(1l);

        // Outlinks populate
        outLinks = new LinkedList<String>();

        // DanglingNodeValue
        Text danglingNodeValue = new Text();
        inLinkPage = key;
        danglingNodeValue.set(inLinkPage + " " + false);
        Iterator<Text> pageIterator = values.iterator();

        // Get the node which has the Unique Id first and save it in thePage
        // else populate all the inlinks to the same page Id
        while (pageIterator.hasNext()) {
            String page[] = pageIterator.next().toString().split("#");
            boolean isoutLink = Boolean.parseBoolean(page[3]);
            long index = Long.parseLong(page[2]);
            String Cj = page[4];
            if (isoutLink) {
                outLinks.add(index + "~" + Cj);
            } else {
                thePage = "node# " + page[2] + " " + page[1] + " " + page[2] + " " + page[3] + " " + page[4];
            }
        }

        try {
            // Default Mapper for Inverse mapper
            Text defaultKeyForInversionMapper = new Text();
            defaultKeyForInversionMapper.set("MAPPER");
            String inverseMapper = "";
            Text index = new Text();

            // Page is not crawled and is a not part of the dataset // generate index
            if (thePage == null) {

                Long newIndex = context.getCounter(Counter.INDEX).getValue();
                context.getCounter(Counter.INDEX).increment(1l);
                index.set(newIndex + "");
                inverseMapper = "inlink#" + newIndex + "->" + inLinkPage;

            }
            // Page is a crawled and is a part of the dataset
            else {
                // basically the index that came from mapper
                String indexOfPage = thePage.split(" ")[1];
                index.set(indexOfPage + "");
                inverseMapper = thePage;
            }

            // Emit a copy of page with index for inversion mapping "MAPPER" : "Index->PageName"
            Text inverseMapperValue = new Text();
            inverseMapperValue.set(inverseMapper);
            context.write(index, inverseMapperValue);


            /// now generate the PR vector
            Text initialPageRank = new Text();
            initialPageRank.set("PR#" + 0.0);
            context.write(index, initialPageRank);

            // Emit inlink node
            Text inlinks = new Text();
            inlinks.set("inlink#" + outLinks + "");
            context.write(index, inlinks);

        } catch (Exception exp) {
            System.out.println("Exception in cleanup parser" + exp);
        }

    }


}
