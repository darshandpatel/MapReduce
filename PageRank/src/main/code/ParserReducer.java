package code;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Created by Darshan on 10/22/16.
 */
public class ParserReducer extends Reducer<Text, Node, Text, Node> {

    Node returnNode = new Node();
    List<Text> adjNodes = new LinkedList<Text>();

    public void reduce(Text key, Iterable<Node> nodes,
                       Context context) throws IOException, InterruptedException {

        /**
         * If current same page exists multiple time in the source file then only consider once.
         */
        for(Node node: nodes){
            adjNodes = node.getAdjacencyNodes();
            break;
        }

        returnNode.setPageRank(0);
        returnNode.setIsOnlyPageRankContribution(false);
        returnNode.setAdjacencyNodes(adjNodes);
        context.write(key, returnNode);
    }
}
