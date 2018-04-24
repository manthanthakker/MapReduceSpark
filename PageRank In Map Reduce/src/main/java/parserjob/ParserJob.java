package parserjob;

import enums.PageRankEnums;
import model.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author Manthan Thakker
 * @project HW3
 * @date 2/20/18
 * @email thakker.m@husky.neu.edu
 */
public class ParserJob {

    private static Pattern namePattern;
    private static Pattern linkPattern;

    static {
        // Keep only html pages not containing tilde (~).
        namePattern = Pattern.compile("^([^~]+)$");
        // Keep only html filenames ending relative paths and not containing tilde (~).
        linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
    }


    public static class ParserMapper extends Mapper<LongWritable, Text, Text, Node> {

        private Set uniquePages;

        public void setup(Context context) {
            uniquePages = new HashSet();

        }

        public void map(LongWritable key, Text node, Context context) throws IOException, InterruptedException {


            try {
                // Configure parser.
                SAXParserFactory spf = SAXParserFactory.newInstance();
                spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
                SAXParser saxParser = spf.newSAXParser();
                XMLReader xmlReader = saxParser.getXMLReader();
                // Parser fills this list with linked page names.
                List<String> linkPageNames = new LinkedList<String>();
                xmlReader.setContentHandler(new WikiParser(linkPageNames));


                String line = node.toString();

                Text keyToEmit = new Text();


                // Each line formatted as (Wiki-page-name:Wiki-page-html).
                int delimLoc = line.indexOf(':');
                String page = line;
                // replace & with &amp
                line = line.replaceAll("&", "&amp;").trim();
                String pageName = line.substring(0, delimLoc);
                String html = line.substring(delimLoc + 1);
                Matcher matcher = namePattern.matcher(pageName);
                if (!matcher.find()) {
                    // Skip this html file, name contains (~).
                    return;
                }


                // Parse page and fill list of linked pages.
                linkPageNames.clear();
                try {
                    xmlReader.parse(new InputSource(new StringReader(html)));
                } catch (Exception e) {
                    // Discard ill-formatted pages.
                    return;
                }

                Node newNode = new Node(pageName);

                Set pagesSet=new HashSet();
                pagesSet.addAll(linkPageNames);

                newNode.neighbors = new LinkedList<String>(pagesSet);
                uniquePages.add(pageName);
                newNode.pageRank=-1.0;
                keyToEmit.set(pageName);
                context.write(keyToEmit, newNode);


            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void cleanup(Context context) {
            context.getCounter(PageRankEnums.UNIQUEPAGES).increment(uniquePages.size());
        }

        /**
         * Parses a Wikipage, finding links inside bodyContent div element.
         */
        private static class WikiParser extends DefaultHandler {
            /**
             * List of linked pages; filled by parser.
             */
            private List<String> linkPageNames;
            /**
             * Nesting depth inside bodyContent div element.
             */
            private int count = 0;

            public WikiParser(List<String> linkPageNames) {
                super();
                this.linkPageNames = linkPageNames;
            }

            @Override
            public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
                super.startElement(uri, localName, qName, attributes);
                if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
                    // Beginning of bodyContent div element.
                    count = 1;
                } else if (count > 0 && "a".equalsIgnoreCase(qName)) {
                    // Anchor tag inside bodyContent div element.
                    count++;
                    String link = attributes.getValue("href");
                    if (link == null) {
                        return;
                    }

                    try {
                        // Decode escaped characters in URL.
                        link = URLDecoder.decode(link, "UTF-8");
                    } catch (Exception e) {
                        // Wiki-weirdness; use link as is.
                    }
                    // Keep only html filenames ending relative paths and not containing tilde (~).
                    Matcher matcher = linkPattern.matcher(link);
                    if (matcher.find()) {
                        linkPageNames.add(matcher.group(1));
                    }
                } else if (count > 0) {
                    // Other element inside bodyContent div.
                    count++;
                }
            }

            @Override
            public void endElement(String uri, String localName, String qName) throws SAXException {
                super.endElement(uri, localName, qName);
                if (count > 0) {
                    // End of element inside bodyContent div.
                    count--;
                }
            }
        }
    }


    public static long main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Parser");

        // Setup
        job.setJarByClass(ParserJob.class);
        job.setMapperClass(ParserMapper.class);


        //Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Node.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/0"));
        job.waitForCompletion(true);
        return job.getCounters().findCounter(PageRankEnums.UNIQUEPAGES).getValue();


    }
}
