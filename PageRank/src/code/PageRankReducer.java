package code;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PageRankReducer extends Reducer<Text, Node, Text, Node>{

	int iteration;
	float alpha;
	long pageCount;
	long danglingNodeCounter;
	long danglingNodesPRSum;
	long constantTerm;
	Node nodeWithAdjNodes = new Node();
	
	public void setup(Context context){
		Configuration conf = context.getConfiguration();
		alpha = conf.getLong(Constant.ALPHA, (long) 0.15);
		iteration = conf.getInt(Constant.ITERATION, -10);
		pageCount = conf.getInt(Constant.PAGE_COUNT, -10);
		//danglingNodeCounter = conf.getInt(Constant.DANGLING_NODE_COUNTER, -10);
		//danglingNodesPRSum = conf.getInt(Constant.DANGLING_NODES_PR_SUM, -10);
		
		if (iteration == -10 || pageCount == -10 || danglingNodeCounter == -10 || danglingNodesPRSum == -10) {
			throw new Error("Didn't propagate on Page Rank Reducer");
		}
		
		constantTerm = (long) ((alpha/pageCount) + (1-alpha)*(danglingNodesPRSum/pageCount));
		System.out.println("Constant Term value is : " + constantTerm);
	}
	
	public void reduce(Text key, Iterable<Node> nodes,  
			Context context) throws IOException, InterruptedException {
		
		// Calculate New Page Rank
		long pageRankSum = 0L;
		boolean deadNode = true;
		for(Node node : nodes){
			
			if(node.isOnlyPageRank()){
				pageRankSum += node.getPageRank();
			} else {
				nodeWithAdjNodes.setAdjacencyNodes(node.getAdjacencyNodes());
				deadNode = false;
			}
		}
		
		if(deadNode){
			return;
		}
		
		long newPageRank = (long) (constantTerm + (1-alpha)*pageRankSum);
		nodeWithAdjNodes.setPageRank(newPageRank);
		
		// Calculate Sum of PR of dangling nodes
		if(nodeWithAdjNodes.getAdjacencyNodes().size() == 0){
			context.getCounter(COUNTERS.DANGLING_NODE_PR_SUM).increment(newPageRank);
		}
		
		context.write(key, nodeWithAdjNodes);
	}
}
