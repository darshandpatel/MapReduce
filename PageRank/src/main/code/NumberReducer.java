package code;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Darshan on 10/23/16.
 */
public class NumberReducer extends Reducer<Text, Node, Text, Node> {

    Node normalNode = new Node();
    Node emptyNode = new Node();

    public void reduce(Text key, Iterable<Node> nodes,
                       Context context) throws IOException, InterruptedException {

        boolean isDanglingNode = true;

        for(Node node: nodes){

            if(node.getAdjacencyNodes().size() != 0){
                isDanglingNode = false;
                normalNode.setAdjacencyNodes(node.getAdjacencyNodes());
                normalNode.setPageRank(0);
                break;
            }

        }
        if(isDanglingNode){
            context.write(key, emptyNode);
            context.getCounter(COUNTERS.NUMBER_OF_DANGLING_NODE).increment(1l);
        }else{
            context.write(key, normalNode);
        }
        // Keep track of how many pages exists in the source
        context.getCounter(COUNTERS.PAGE_COUNTER).increment(1l);
    }
}
