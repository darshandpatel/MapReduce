package code;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * Created by Darshan on 10/22/16.
 */
public class ParserReducer {

    Node returnNode = new Node();

    public void reduce(Text key, Iterable<Node> nodes,
                       Reducer.Context context) throws IOException, InterruptedException {

        Set<Text> uniqueAdjNodes = new HashSet<Text>();
        for(Node node: nodes){
            uniqueAdjNodes.addAll(node.getAdjacencyNodes());
        }

        returnNode.setPageRank(0);
        returnNode.setIsOnlyPageRankContribution(false);
        returnNode.setAdjacencyNodes(new LinkedList<Text>(uniqueAdjNodes));
        context.write(key, returnNode);
        // Keep track of how many pages exists in the source
        context.getCounter(COUNTERS.PAGE_COUNTER).increment(1);
    }
}
