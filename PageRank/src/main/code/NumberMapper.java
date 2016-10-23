package code;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

/**
 * Created by Darshan on 10/23/16.
 */
public class NumberMapper extends Mapper<Object, Text, Text, Node> {

    Node node = new Node();

    public void map(Text key, Node value, Context context) throws IOException, InterruptedException {

        for(Text adjNode : value.getAdjacencyNodes()){
            context.write(adjNode, node);
        }
        context.write(key, value);

    }


}
