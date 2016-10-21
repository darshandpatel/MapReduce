package code;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TopMapper extends Mapper<Text, Node, LongWritable, Text>{
	
	LongWritable pageRank = new LongWritable();
	public void map(Text key, Node value, Context context) throws IOException, InterruptedException{
		
		pageRank.set((long) value.getPageRank());
		context.write(pageRank, key);
	}

}
