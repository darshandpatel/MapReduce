package code;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PageRankReducer extends Reducer<Text, Node, Text, Node>{

	int iteration;
	double alpha;
	long pageCount;
	double danglingNodesPRSum;
	double constantTerm;
	Node nodeWithAdjNodes = new Node();
	
	public void setup(Context context){
		Configuration conf = context.getConfiguration();
		alpha = conf.getDouble(Constant.ALPHA, 0.15);
		iteration = conf.getInt(Constant.ITERATION, -10);
		pageCount = conf.getLong(Constant.PAGE_COUNT, -10L);
		danglingNodesPRSum = conf.getDouble(Constant.DANGLING_NODES_PR_SUM, 0);
		danglingNodesPRSum = danglingNodesPRSum / Math.pow(10, 12);
		
		if (iteration == -10 || pageCount == -10) {
			throw new Error("Didn't propagate on Page Rank Reducer");
		}
		
		constantTerm = ((alpha/pageCount) + (1-alpha)*(danglingNodesPRSum/pageCount));
		System.out.println("Constant Term value is : " + constantTerm);
	}
	
	public void reduce(Text key, Iterable<Node> nodes,  
			Context context) throws IOException, InterruptedException {
		
		// Calculate New Page Rank
		double pageRankSum = 0d;
		boolean deadNode = true;
		for(Node node : nodes){
			
			if(node.isOnlyPageRankContribution()){
				pageRankSum += node.getPageRankContribution();
			} else {
				nodeWithAdjNodes.setAdjacencyNodes(node.getAdjacencyNodes());
				deadNode = false;
			}
		}
		
		if(deadNode){
			return;
		}
		
		double newPageRank = (constantTerm + (1-alpha)*pageRankSum);
		nodeWithAdjNodes.setPageRank(newPageRank);
		
		// Calculate Sum of PR of dangling nodes
		if(nodeWithAdjNodes.getAdjacencyNodes().size() == 0){
			context.getCounter(COUNTERS.DANGLING_NODE_PR_SUM).increment((long)(newPageRank * Math.pow(10, 12)));
		}
		
		context.write(key, nodeWithAdjNodes);
	}
}
