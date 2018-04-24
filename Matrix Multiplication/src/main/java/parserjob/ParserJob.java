package parserjob;

import enums.Counter;
import enums.PageRankEnums;
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

    /**
     * Parses each line and outputs the record
     */
    public static class ParserMapper extends Mapper<LongWritable, Text, Text, Text> {


        public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {


            try {

                //preprocessing
                String line = record.toString();
                int delimLoc = line.indexOf(':');
                line = line.replaceAll("&", "&amp;").trim();
                String inPageName = line.substring(0, delimLoc);
                String html = line.substring(delimLoc + 1);
                //preprocessing


                // parse HTML and get outlinks
                Set<String> outPages = parse(inPageName, html);
                Long index = Long.parseLong(key + "");



                // InPage prepration (PageName,(#pageName#index#false))
                Text inPageKey = new Text();
                Text inPageValue = new Text();
                inPageKey.set(inPageName + "");
                inPageValue.set("#" + inPageName + "#" + index + "#" + false + "#" + outPages.size());

                context.write(inPageKey, inPageValue);

                // Emitting the page which is an outlink (Outlink,IndexOfInPage)
                // Outlinks
                Text outlinkKey = new Text();
                Text outlinkValue = new Text();
                Iterator<String> outlinksIterator = outPages.iterator();
                while (outlinksIterator.hasNext()) {
                    String pageNameOfOutlink = outlinksIterator.next();
                    // Key
                    outlinkKey.set(pageNameOfOutlink.trim());
                    // value

                    outlinkValue.set("#" + inPageName.trim() + "#" + index.toString().trim() + "#" + true + "#" + outPages.size());
                    // Emit
                    context.write(outlinkKey, outlinkValue);
                    /// In output tru fiels name mentions from where it came on
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
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

    /**
     * @param pageName: PageName
     * @param html:     Html to parse
     * @return Set of Pages as outlinks
     */
    public static Set<String> parse(String pageName, String html) {
        Set pagesSet = new HashSet();
        try {
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            SAXParser saxParser = spf.newSAXParser();
            XMLReader xmlReader = saxParser.getXMLReader();
            // Parser fills this list with linked page names.
            List<String> linkPageNames = new LinkedList<String>();

            xmlReader.setContentHandler(new ParserMapper.WikiParser(linkPageNames));

            Matcher matcher = namePattern.matcher(pageName);
            if (!matcher.find()) {
                // Skip this html file, name contains (~).
                return pagesSet;
            }


            // Parse page and fill list of linked pages.
            try {
                xmlReader.parse(new InputSource(new StringReader(html)));
            } catch (Exception e) {
                // Discard ill-formatted pages.
                return pagesSet;
            }
            pagesSet.addAll(linkPageNames);

            linkPageNames.clear();
            return pagesSet;
        } catch (Exception e) {
            System.out.println("Error in parsing" + e);
        }
        return pagesSet;
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Parser");

        // Setup
        job.setJarByClass(ParserJob.class);
        job.setMapperClass(ParserMapper.class);

        //Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ParserReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        job.getCounters().findCounter(Counter.INDEX).setValue(1000000l);
        System.out.println(job.getCounters().findCounter(PageRankEnums.UNIQUEPAGES).getValue());


    }


}
