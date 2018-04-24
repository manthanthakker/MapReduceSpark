package inversemapper;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;


/**
 * @author Manthan Thakker
 * @project HW5
 * @date 3/29/18
 * @email thakker.m@husky.neu.edu
 */

/**
 * "PR#"+0.0
 */
public class InverseMapper extends Mapper<LongWritable, Text, Text, Text> {

    HashMap<Long,String> inverseMapper;


    public void setup(Context context){
            populateCache(context);

    }

    void populateCache(Context context) {
        BufferedReader reader=null;
        try {
            URI[] stopWordsFiles = DistributedCache.getCacheFiles(context.getConfiguration());
            System.out.println(stopWordsFiles);

            // Direct access by name
            File baz = new File("mapper/part-r-00000");

            if (stopWordsFiles != null && stopWordsFiles.length > 0) {
                System.out.println("path is "+stopWordsFiles[0]);
                reader = new BufferedReader(new FileReader(baz));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length == 2) {
                        Long key = Long.parseLong(parts[0]);

                        inverseMapper.put(key, parts[1]);
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

        }

    }

    public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {

        String pageRankPageName[] = record.toString().split("\t");

        String pageName=inverseMapper.getOrDefault(Long.parseLong(pageRankPageName[1]),"NOTFOUND");
        Text pageRank=new Text();
        pageRank.set(pageRankPageName[0]);
        Text pageNameText=new Text();
        pageNameText.set(pageName);

        context.write(pageNameText,pageRank);


    }


}
