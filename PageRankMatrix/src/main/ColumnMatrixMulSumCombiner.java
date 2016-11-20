package main;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ColumnMatrixMulSumCombiner extends Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {
	
	DoubleWritable pageRankContrSum = new DoubleWritable();
	
	public void reduce(LongWritable key, Iterable<DoubleWritable> values,
            Context context) throws IOException, InterruptedException {
		
		Double totalPRContribution = 0d;
		for(DoubleWritable prContribution : values){
			totalPRContribution += prContribution.get();
		}
		pageRankContrSum.set(totalPRContribution);
		context.write(key, pageRankContrSum);
	}

}
