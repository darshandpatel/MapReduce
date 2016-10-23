package code;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by Darshan on 10/23/16.
 * This Mapper emits the adjacency node as key and empty node (Without adjacency node list)
 * as their value for the given souce node/page
 *
 * Example:
 * Input Key: A  Value : Node[B, C, D]
 * Emits:
 * A [B, C, D]
 * B []
 * C []
 * D []
 *
 * Due to this form of output reduce would be able to identify a dead node
 * and convert them into dangling node.
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
