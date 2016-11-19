package main;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ColumnMatrixMulReducer extends Reducer<Cell, Cell, LongWritable, DoubleWritable> {
	
	DoubleWritable pageRankContribution = new DoubleWritable();
	LongWritable index = new LongWritable();
	int iteration;
	long nbrOfDanglingNode;
	long pageCount;
	
	public void setup(Context context) {
		Configuration conf =  context.getConfiguration();
		iteration = conf.getInt(Constant.ITERATION, -10);
		nbrOfDanglingNode = conf.getLong(Constant.NUMBER_OF_DANGLING_NODE, -10);
		pageCount = conf.getLong(Constant.PAGE_COUNT, -10);
		
	}
    
	public void reduce(Cell key, Iterable<Cell> values,
            Context context) throws IOException, InterruptedException {
		
		Double currentPageRank = 0d;
		boolean first = true;
		boolean deadNode = true;
		for(Cell cell : values){
			
			if(first){
				currentPageRank = cell.getPageRank();
				//System.out.println(" Current Page Rank : "+currentPageRank);
				first = false;
			}else{
				deadNode = false;
				pageRankContribution.set(cell.getContribution() * currentPageRank);
				index.set(cell.getIndex());
				//System.out.print("PRContribution : " + cell.getContribution() * currentPageRank);
				context.write(index, pageRankContribution);
			}
		}
		//System.out.println("");
		if(deadNode){
			pageRankContribution.set(0l);
			index.set(key.getIndex());
			context.write(index, pageRankContribution);
		}
		
	}

}
