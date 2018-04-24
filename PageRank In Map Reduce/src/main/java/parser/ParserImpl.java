package parser;

import java.io.*;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/**
 * @author Manthan Thakker
 * @project HW3
 * @date 2/19/18
 * @email thakker.m@husky.neu.edu
 */
public class ParserImpl implements Parser {
    private static Pattern namePattern;
    private static Pattern linkPattern;

    static {
        // Keep only html pages not containing tilde (~).
        namePattern = Pattern.compile("^([^~]+)$");
        // Keep only html filenames ending relative paths and not containing tilde (~).
        linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
    }

    public static void main(String[] args) {

        // Path of the file
        String path = "/Users/trailbrazer/Desktop/MR/HW3/input/wikipedia-simple-html.bz2";


        long count=0;

        BufferedReader reader = null;
        try {
            File inputFile = new File(path);
            if (!inputFile.exists() || inputFile.isDirectory() || !inputFile.getName().endsWith(".bz2")) {
                System.out.println("Input File does not exist or not bz2 file: " + path);
                System.exit(1);
            }

            // Configure parser.
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            SAXParser saxParser = spf.newSAXParser();
            XMLReader xmlReader = saxParser.getXMLReader();
            // Parser fills this list with linked page names.
            List<String> linkPageNames = new LinkedList();
            xmlReader.setContentHandler(new WikiParser(linkPageNames));

            BZip2CompressorInputStream inputStream = new BZip2CompressorInputStream(new FileInputStream(inputFile));
            reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;

            while ((line = reader.readLine()) != null) {
                count++;
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
                    continue;
                }

                // Parse page and fill list of linked pages.
                linkPageNames.clear();
                try {
                    xmlReader.parse(new InputSource(new StringReader(html)));
                } catch (Exception e) {
                    // Discard ill-formatted pages.
                    continue;
                }

            }

        }
        catch (EOFException e) {

        }catch (Exception e) {
            e.printStackTrace();
        }

        finally
         {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
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
