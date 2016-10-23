package code;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Darshan on 10/23/16.
 * This Reducer main purpose is to convert dead node into dangling node
 * and calculate total number of nodes/pages and dangling node in the
 * source graph.
 */
public class NumberReducer extends Reducer<Text, Node, Text, Node> {

    Node normalNode = new Node();
    Node emptyNode = new Node();

    public void reduce(Text key, Iterable<Node> nodes,
                       Context context) throws IOException, InterruptedException {

        boolean isDeadDanglingNode = true;

        for(Node node: nodes){

            if(node.getAdjacencyNodes().size() != 0){
                isDeadDanglingNode = false;
                normalNode.setAdjacencyNodes(node.getAdjacencyNodes());
                normalNode.setPageRank(0);
                break;
            }
            // Dead node and dangling node will have empty adjancey list

        }

        // For the dead and danlging node emit the value as a node with empty adjacency list.
        if(isDeadDanglingNode){
            context.write(key, emptyNode);
            context.getCounter(COUNTERS.NUMBER_OF_DANGLING_NODE).increment(1l);
        }else{
            context.write(key, normalNode);
        }
        // Keep track of how many pages exists in the source
        context.getCounter(COUNTERS.PAGE_COUNTER).increment(1l);
    }
}
