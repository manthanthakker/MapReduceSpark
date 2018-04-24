package package1;

import java.io.*;
import java.net.URLDecoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;

/**
 * @author Manthan Thakker
 * @project HW3
 * @date 2/19/18
 * @email thakker.m@husky.neu.edu
 */
public class ParserImpl {
    /**
     * Parser
     */
    private static Pattern namePattern;
    private static Pattern linkPattern;
    SAXParserFactory spf;
    SAXParser saxParser;
    XMLReader xmlReader;

    /**
     * Constructor to initialize the references
     */
    public ParserImpl() {
        spf = SAXParserFactory.newInstance();

        try {
            spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            saxParser = null;
            // keep this here
            saxParser = spf.newSAXParser();
            xmlReader = saxParser.getXMLReader();


        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXNotRecognizedException e) {
            e.printStackTrace();
        } catch (SAXNotSupportedException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        }
    }

    static {
        // Keep only html pages not containing tilde (~).
        namePattern = Pattern.compile("^([^~]+)$");
        // Keep only html filenames ending relative paths and not containing tilde (~).
        linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");


    }


    /**
     * Parses a Wikipage, finding links inside bodyContent div element.
     */
    private static class WikiParser extends DefaultHandler {
        /**
         * List of linked pages; filled by parser.
         */
        private Set<String> linkPageNames;
        /**
         * Nesting depth inside bodyContent div element.
         */
        private int count = 0;

        public WikiParser(Set<String> linkPageNames) {
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
                    linkPageNames.add(matcher.group(1).trim());
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


    /**
     * Given a single line from the input file
     * returns the pageName and the ite neighbor
     *
     * @param line: Input line from input file
     * @return (pageName, Outlinks)
     */
    public NodeIdNeighbors parse(String line) {
        try {


            // Each line formatted as (Wiki-page-name:Wiki-page-html).
            int delimLoc = line.indexOf(':');

            String pageName = line.substring(0, delimLoc).trim();
            String html = line.substring(delimLoc + 1);

            Matcher matcher = namePattern.matcher(pageName);
            if (!matcher.find()) {
                // Skip this html file, name contains (~).
                return null;
            }

            // Parser fills this list with linked page names.
            Set<String> linkPageNames = new HashSet<>();
            xmlReader.setContentHandler(new WikiParser(linkPageNames));

            // Parse page and fill list of linked pages.
            linkPageNames.clear();

            try {
                xmlReader.parse(new InputSource(new StringReader(html)));
            } catch (Exception e) {
                // Discard ill-formatted pages.

            }
            NodeIdNeighbors newNode = new NodeIdNeighbors(pageName, linkPageNames);

            return newNode;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

}