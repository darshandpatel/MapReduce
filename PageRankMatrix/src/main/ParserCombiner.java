package main;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by Darshan on 10/22/16.
 */
public class ParserCombiner extends Reducer<Text, Node, Text, Node> {

    //List<Text> adjNodes = new LinkedList<Text>();
    Node normalNode = new Node();
    Node emptyNode = new Node();
    
    public void setup(Context context){
    	System.out.println("ParserCombiner is called");
    }
    
    public void reduce(Text key, Iterable<Node> nodes,
                       Context context) throws IOException, InterruptedException {

    	 boolean isDeadDanglingNode = true;
        
        for(Node node: nodes){

            if(node.getAdjacencyNodes().size() != 0){
                isDeadDanglingNode = false;
                normalNode.setAdjacencyNodes(node.getAdjacencyNodes());
                break;
            }
        }
        
        // For the dead and danlging node emit the value as a node with empty adjacency list.
        if(isDeadDanglingNode){
            context.write(key, emptyNode);
        }else{
            context.write(key, normalNode);
        }
    }
}