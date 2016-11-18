package main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class IdReducer extends Reducer<Text, Node, Text, Text> {
	
	long id;
	Text idPlusRank = new Text();
	Text danglingId = new Text();
	Long pageCount;
	
	private MultipleOutputs multipleOutputs;
	
	public void setup(Context context){
		id = 1;
		Configuration conf = context.getConfiguration();
		pageCount = conf.getLong(Constant.PAGE_COUNT, -10);
		multipleOutputs = new MultipleOutputs(context);
	}
	
	public void reduce(Text key, Iterable<Node> pages,
            Context context) throws IOException, InterruptedException {
		
		for(Node page : pages){
			//idWritable.set(id);
			if(page.isDangling()){
				danglingId.set(Long.toString(id));
				multipleOutputs.write(Constant.DANGLING_MO, page.getPageName(), danglingId);
			}
			idPlusRank.set(id+"\t"+(1d/pageCount));
			multipleOutputs.write(Constant.IDS_MO, page.getPageName(), idPlusRank);
			id++;
		}
	}
	
	 public void cleanup(Context contet) throws IOException, InterruptedException {
		 multipleOutputs.close();
    }
}
