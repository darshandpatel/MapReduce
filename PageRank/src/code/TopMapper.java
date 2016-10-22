package code;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TopMapper extends Mapper<Text, Node, DoubleWritable, Text>{
	
	DoubleWritable pageRank = new DoubleWritable();
	public void map(Text key, Node value, Context context) throws IOException, InterruptedException{
		
		pageRank.set(value.getPageRank());
		context.write(pageRank, key);
	}

}


class SampleMapper extends Mapper<Text, Node, Text, DoubleWritable>{
	
	DoubleWritable pageRank = new DoubleWritable();
	public void map(Text key, Node value, Context context) throws IOException, InterruptedException{
		
		pageRank.set(value.getPageRank());
		context.write(key, pageRank);
	}

}
