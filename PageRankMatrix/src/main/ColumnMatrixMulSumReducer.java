package main;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ColumnMatrixMulSumReducer extends Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {
	
	DoubleWritable newPageRank = new DoubleWritable();
	
	Long pageCount;
	Double alpha;
	
	public void setup(Context context){
		Configuration conf =  context.getConfiguration();
		pageCount = conf.getLong(Constant.PAGE_COUNT, -10);
		alpha = conf.getDouble(Constant.ALPHA, -10);
	}
    
	public void reduce(LongWritable key, Iterable<DoubleWritable> values,
            Context context) throws IOException, InterruptedException {
		
		Double totalPRContribution = 0d;
		for(DoubleWritable prContribution : values){
			totalPRContribution += prContribution.get();
		}
		newPageRank.set(alpha/pageCount + (1-alpha)*totalPRContribution);
		context.write(key, newPageRank);
	}

}
