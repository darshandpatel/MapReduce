package code;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Node, Text, Node> {
	
	int iteration;
	int pageCount;
	Node pageRankContributionNode = new Node();
	
	public void setup(Context context){
		Configuration conf = context.getConfiguration();
		iteration = conf.getInt(Constant.ITERATION, -10);
		pageCount = conf.getInt(Constant.PAGE_COUNT, -10);
		
		if (iteration == -10 || pageCount == -10) {
			throw new Error("Didn't propagate iteration or page count");
		}
	}

	public void map(Text key, Node value, Context context) throws IOException, InterruptedException{
		
		if(iteration == 0){
			value.setPageRank(((1d/pageCount))); // * Math.pow(10, 12)
		}
		
		int adjLen = value.getAdjacencyNodes().size();
		
		for(Text adjPageName : value.getAdjacencyNodes()){
			pageRankContributionNode.setIsOnlyPageRankContribution(true);
			pageRankContributionNode.setPageRankContribution(value.getPageRank()/adjLen);
			//System.out.println("Adjacney node size : "+adjLen + " Current Page Rank : " +value.getPageRank());
			//System.out.println("Contribution : "+value.getPageRank()/adjLen);
			context.write(adjPageName, pageRankContributionNode);
		}
		
		context.write(key, value);
	}
}
