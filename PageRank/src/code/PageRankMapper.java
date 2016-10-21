package code;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PageRankMapper extends Mapper<Text, Node, Text, Node> {

	public void map(Object key, Text line, Context context) throws IOException, InterruptedException{
		
	}
}
