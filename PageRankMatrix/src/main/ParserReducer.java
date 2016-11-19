package main;

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

    //List<Text> adjNodes = new LinkedList<Text>();
    Node normalNode = new Node();
    Node emptyNode = new Node();
    
    public void reduce(Text key, Iterable<Node> nodes,
                       Context context) throws IOException, InterruptedException {

    	 boolean isDeadDanglingNode = true;
        /**
         * If current same page exists multiple time in the source file then only consider once.
         */
        //for(Node node: nodes){
        //    adjNodes = node.getAdjacencyNodes();
        //    break;
        //}
        
        for(Node node: nodes){

            if(node.getAdjacencyNodes().size() != 0){
                isDeadDanglingNode = false;
                normalNode.setAdjacencyNodes(node.getAdjacencyNodes());
                break;
            }
            // Dead node and dangling node will have empty adjancey list

        }

        //returnNode.setPageRank(0);
        //returnNode.setIsOnlyPageRankContribution(false);
        //returnNode.setAdjacencyNodes(adjNodes);
        
        // For the dead and danlging node emit the value as a node with empty adjacency list.
        if(isDeadDanglingNode){
            context.write(key, emptyNode);
            context.getCounter(COUNTERS.NUMBER_OF_DANGLING_NODE).increment(1l);
        }else{
            context.write(key, normalNode);
        }
        // Keep track of how many pages exists in the source
        context.getCounter(COUNTERS.PAGE_COUNTER).increment(1l);
        
        //context.write(key, returnNode);
    }
}