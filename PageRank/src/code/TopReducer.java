package code;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopReducer extends Reducer<LongWritable, Text, Text, LongWritable>{
	
	int counter;
	public void setup(){
		counter=0;
	}
	
	public void reduce(LongWritable key, Iterable<Text> pages,  
			Context context) throws IOException, InterruptedException {
		
		if(counter < 100){
			for(Text page : pages){
				context.write(page, key);
				counter+=1;
				if(counter > 99){
					break;
				}
			}
		}
		
	}
}
