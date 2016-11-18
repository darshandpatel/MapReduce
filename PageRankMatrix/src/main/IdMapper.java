package main;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IdMapper extends Mapper<Text, Node, Text, Node>{

	Text dummyKey = new Text(Constant.NORMAL_DUMMY_KEY);
	Node node = new Node();
	
	public void map(Text key, Node value, Context context) throws IOException, InterruptedException {
		
		node.setPageName(key.toString());
		if(value.getAdjacencyNodes().size() == 0){
			node.setIsDangling(true);
			context.write(dummyKey, node);
		}else{
			node.setIsDangling(false);
			context.write(dummyKey, node);
		}
    }
}
