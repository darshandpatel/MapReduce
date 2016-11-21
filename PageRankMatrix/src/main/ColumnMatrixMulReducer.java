package main;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
	HashMap<Long, Double> hashMap;
	
	public void setup(Context context) {
		Configuration conf =  context.getConfiguration();
		iteration = conf.getInt(Constant.ITERATION, -10);
		nbrOfDanglingNode = conf.getLong(Constant.NUMBER_OF_DANGLING_NODE, -10);
		pageCount = conf.getLong(Constant.PAGE_COUNT, -10);
		hashMap = new HashMap<Long, Double>();
		
	}
    
	public void reduce(Cell key, Iterable<Cell> values,
            Context context) throws IOException, InterruptedException {
		
		Double currentPageRank = 0d;
		boolean first = true;
		for(Cell cell : values){
			
			if(first){
				currentPageRank = cell.getPageRank();
				first = false;
			}else{
				//pageRankContribution.set(cell.getContribution() * currentPageRank);
				//index.set(cell.getIndex());
				//System.out.print("PRContribution : " + cell.getContribution() * currentPageRank);
				
				if(hashMap.containsKey(cell.getIndex())){
					hashMap.put(cell.getIndex(), hashMap.get(cell.getIndex())+(cell.getContribution() * currentPageRank));
				}else{
					hashMap.put(cell.getIndex(), cell.getContribution() * currentPageRank);
				}
				//context.write(index, pageRankContribution);
			}
		}
		if(!hashMap.containsKey(key.getIndex())){
			hashMap.put(key.getIndex(), 0d);
		}
		//System.out.println("");
		//pageRankContribution.set(0l);
		//index.set(key.getIndex());
		//context.write(index, pageRankContribution);
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		Iterator<Map.Entry<Long, Double>> iterator = hashMap.entrySet().iterator();
		while(iterator.hasNext()){
			Map.Entry<Long, Double> pair = iterator.next();
			index.set(pair.getKey());
			pageRankContribution.set(pair.getValue());
			context.write(index, pageRankContribution);
		}
	}
}
