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
		iteration = Integer.parseInt(conf.get(Constant.ITERATION));
		alpha = Long.parseLong(conf.get(Constant.ALPHA));
		pageCount = Integer.parseInt(conf.get(Constant.PAGE_COUNT));
		danglingNodeCounter = Integer.parseInt(conf.get(Constant.DANGLING_NODE_COUNTER));
		danglingNodesPRSum = Long.parseLong(conf.get(Constant.DANGLING_NODES_PR_SUM));
		constantTerm = (long) ((alpha/pageCount) + (1-alpha)*(danglingNodesPRSum/pageCount));
	}
	
	public void reduce(Text key, Iterable<Node> nodes,  
			Context context) throws IOException, InterruptedException {
		
		// Calculate New Page Rank
		long prSum = 0L;
		for(Node node : nodes){
			
			if(node.getAdjacencyNodes() == null){
				prSum += node.getPageRank();
			}
			else{
				nodeWithAdjNodes.setAdjacencyNodes(node.getAdjacencyNodes());
			}
		}
		
		long newPageRank = (long) (constantTerm + (1-alpha)*prSum);
		nodeWithAdjNodes.setPageRank(newPageRank);
		
		// Calculate Sum of PR of dangling nodes
		if(nodeWithAdjNodes.getAdjacencyNodes() == null){
			context.getCounter(COUNTERS.DANGLING_NODE_PR_SUM).increment(newPageRank);
		}
		
		context.write(key, nodeWithAdjNodes);
	}
}
