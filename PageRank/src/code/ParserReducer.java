package code;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParserReducer extends Reducer<Text, Node, Text, Text> {
	
	Text returnText = new Text();
	public void reduce(Text key, Iterable<Node> nodes,  
			Context context) throws IOException, InterruptedException {
		
		context.getCounter(COUNTERS.PAGE_COUNTER).increment(1);
		boolean isDanglingNode = true;
		
		for(Node node : nodes){
			if(node.getAdjacencyNodes() != null && node.getAdjacencyNodes().size() > 0){
				isDanglingNode = false;
				returnText.set(node.toString());
				context.write(key, returnText);
			}
		}
		
		if(isDanglingNode){
			context.getCounter(COUNTERS.DANGLING_NODE_COUNTER).increment(1);
			returnText.set("[]");
			context.write(key, returnText);
		}
	}

}
