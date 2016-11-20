package main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/*
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
*/
public class IdMapper extends Mapper<Text, Node, Text, Text>{
    
	private long id;
    private int increment;
    private MultipleOutputs multipleOutputs;
    Text idPlusRank = new Text();
	Text danglingId = new Text();
	Long pageCount;
	
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	
        Configuration conf = context.getConfiguration();
		pageCount = conf.getLong(Constant.PAGE_COUNT, -10);
		
        id = context.getTaskAttemptID().getTaskID().getId();
        increment = context.getConfiguration().getInt("mapred.map.tasks", 0);
        multipleOutputs = new MultipleOutputs<Text, Text>(context);
        System.out.println("Task id number is : "+id);
        System.out.println("mapred.map.tasks is : "+increment);
        if (increment == 0) {
            throw new IllegalArgumentException("mapred.map.tasks is zero");
        }
        
    }
    @Override
    protected void map(Text key, Node value, Context context)
            throws IOException, InterruptedException {
        id += increment;
        if(value.getAdjacencyNodes().size() == 0){
			danglingId.set(Long.toString(id));
			multipleOutputs.write(Constant.DANGLING_MO, key, danglingId, Constant.DANGLING_MO+"/"+Constant.DANGLING_MO);
		}
		idPlusRank.set(id+"\t"+(1d/pageCount));
		multipleOutputs.write(Constant.IDS_MO, key, idPlusRank, Constant.IDS_MO+"/"+Constant.IDS_MO);
    }
    
    public void cleanup(Context contet) throws IOException, InterruptedException {
 		 multipleOutputs.close();
    }
}
