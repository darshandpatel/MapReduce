package code;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParserReducer extends Reducer<Text, Node, Text, Node> {

    Text returnText = new Text();
    Node emptyNode = new Node();

    public void reduce(Text key, Iterable<Node> nodes,
                       Context context) throws IOException, InterruptedException {

        context.getCounter(COUNTERS.PAGE_COUNTER).increment(1);
        boolean isDanglingNode = true;

        for (Node node : nodes) {
            if (node.getAdjacencyNodes() != null && node.getAdjacencyNodes().size() != 0) {
                isDanglingNode = false;
                context.write(key, node);
                break;
            }
        }

        if (isDanglingNode) {
            context.getCounter(COUNTERS.DANGLING_NODE_COUNTER).increment(1);
            context.write(key, emptyNode);
        }
    }

}
