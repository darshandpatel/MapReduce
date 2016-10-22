package code;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;


public class ParserMapper extends Mapper<Object, Text, Text, Node> {

    private static Pattern namePattern;
    private static Pattern questionPattern;
    private List<String> linkPageNames;
    private XMLReader xmlReader;
    Text returnKey = new Text();
    Node node = new Node();
    Node emptyNode = new Node();

    static {
        // Keep only html pages not containing tilde (~).
        namePattern = Pattern.compile("^([^~]+)$");
        // Keep only html pages not containing only "?" character.
        questionPattern = Pattern.compile("^[? ]*$");
    }

    /**
     * This map method parse the given line from source file and converts into the source page name as key
     * and its adjacency page list as value which is encapsulated in Node Writable object.
     * @param key
     * @param line
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, Text line, Context context) throws IOException, InterruptedException {


        try {

            // Configure parser
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            SAXParser saxParser = spf.newSAXParser();
            xmlReader = saxParser.getXMLReader();
            linkPageNames = new LinkedList<String>();
            xmlReader.setContentHandler(new WikiParser(linkPageNames));

            String lineStr = line.toString();
            int delimLoc = lineStr.indexOf(':');
            String pageName = lineStr.substring(0, delimLoc);
            String html = lineStr.substring(delimLoc + 1);
            Matcher matcher = namePattern.matcher(pageName);
            Matcher questionMatcher = questionPattern.matcher(pageName);

            //if (!matcher.find() || !specialCharMatcher.find()) {
            if (!matcher.find() || questionMatcher.find()) {
                // Skip this html file, name contains (~).
                return;
            }

            // Parse page and fill list of linked pages.
            try {
                html = html.replace("&", "&amp;");
                xmlReader.parse(new InputSource(new StringReader(html)));
            } catch (Exception e) {
                // Discard ill-formatted pages.
                linkPageNames.clear();
            }

            // Set the source page name as key
            returnKey.set(pageName);
            // Remove source page name from its adjacency list if exists.
            linkPageNames = ParserMapper.removeSourcePage(linkPageNames, pageName);

            // If current page doesn't have adjacency pages then emit the node with empty adjacency list
            // and set its page rank to zero and this node is not page rank contribution node (Will be discussed more in
            // Page Rank MR Job Reducer) so set the IsOnlyPageRankContribution flag to false.
            if (linkPageNames.size() == 0) {
                emptyNode.setIsOnlyPageRankContribution(false);
                emptyNode.setPageRank(0);
                context.write(returnKey, emptyNode);
            } else {
                node.setAdjacencyStringNodes(linkPageNames);
                node.setIsOnlyPageRankContribution(false);
                node.setPageRank(0);
                context.write(returnKey, node);
            }

            // Keep track of how many pages exists in the source
            context.getCounter(COUNTERS.PAGE_COUNTER).increment(1);

        } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
    }

    /**
     * This method removes the source page name from its adjacency page lists if exists.
     * @param linkPageNames List of adjacency pages
     * @param sourcePage source page name
     * @return List of adjacency pages which doesn't contain source page name in it.
     */
    public static List<String> removeSourcePage(List<String> linkPageNames, String sourcePage) {

        while (linkPageNames.remove(sourcePage)) {
        }

        return linkPageNames;
    }

}
