package code;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Node, Text, Node> {
	
	int iteration;
	int pageCount;
	Node pageRankNode = new Node();
	
	public void setup(Context context){
		Configuration conf = context.getConfiguration();
		iteration = Integer.parseInt(conf.get("iteration"));
		pageCount = Integer.parseInt(conf.get("pageCount"));
	}

	
	public void map(Text key, Node value, Context context) throws IOException, InterruptedException{
		
		if(iteration == 0){
			value.setPageRank(1.0/pageCount);
		}
		
		int adjLen = value.getAdjacencyNodes().size();
		
		for(Text adjPageName : value.getAdjacencyNodes()){
			pageRankNode.setAdjacencyNodes(null);
			pageRankNode.setPageRank(value.getPageRank()/adjLen);
			context.write(adjPageName, pageRankNode);
		}
		
		context.write(key, value);
	}
}
