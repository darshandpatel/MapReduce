package code;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;



public class ParserMapper extends Mapper<Object, Text, Text, Node> {
	
	private static Pattern namePattern;
	private static Pattern specialCharPattern;
	private static Pattern linkPattern;
	private List<String> linkPageNames;
	private XMLReader xmlReader;
	Text returnKey = new Text();
	Text returnValue = new Text();
	Node node = new Node();
	Node emptyNode = new Node();
	
	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
		specialCharPattern = Pattern.compile("[^A-Za-z0-9]");
	}
	
	public void map(Object key, Text line, Context context) throws IOException, InterruptedException{
		
		// Configure parser.
		try {
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
			//Matcher specialCharMatcher = specialCharPattern.matcher(pageName);
			
			//if (!matcher.find() || !specialCharMatcher.find()) {
			if (!matcher.find()) {
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
			returnKey.set(pageName);
			if(linkPageNames.size() == 0){
				context.write(returnKey, emptyNode);
			}else{
				node.setAdjacencyStringNodes(linkPageNames);
				node.setIsOnlyPageRank(false);
				context.write(returnKey, node);
			}
			context.getCounter(COUNTERS.PAGE_COUNTER).increment(1);
		
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
	}
	
}
