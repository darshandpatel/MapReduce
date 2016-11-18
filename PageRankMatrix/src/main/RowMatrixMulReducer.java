package main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class RowMatrixMulReducer extends Reducer<LongWritable, Cell, LongWritable, DoubleWritable> {
	
	HashMap<Long, Double> pageRank;
	Double mrMulRowValue;
	
	DoubleWritable udpatedPageRank = new DoubleWritable();
	LongWritable row = new LongWritable();
	int iteration;
	double alpha;
    long pageCount;
    
	public void setup(Context context) throws IOException{
		
		pageRank = new HashMap<Long, Double>();
		mrMulRowValue = 0d;
		
		Configuration conf = context.getConfiguration();
        iteration = conf.getInt(Constant.ITERATION, -10);
        pageCount = conf.getLong(Constant.PAGE_COUNT, -10L);
        alpha = conf.getDouble(Constant.ALPHA, -10);
        
		URI[] cacheFiles = context.getCacheFiles();
		FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(cacheFiles[0]));
        
        for (int i=0;i<status.length;i++){
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
            String line;
            line=br.readLine();
            while (line != null){
            	String[] parts = line.split("\t");
    			//System.out.println(parts[0]+" : "+parts[1]+" : "+parts[2]);
    			if(iteration == 1){
    				pageRank.put(Long.parseLong(parts[1]), Double.parseDouble(parts[2]));
    	        }else{
    	        	pageRank.put(Long.parseLong(parts[0]), Double.parseDouble(parts[1]));
    	        }
            }
            br.close();
        }
        
        status = fs.listStatus(new Path(cacheFiles[1]));
        
        for (int i=0;i<status.length;i++){
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
            String line;
            line=br.readLine();
            while (line != null){
            	String[] parts = line.split("\t");
            	Double rank = pageRank.get(Long.parseLong(parts[0]));
            	if(rank == null){
            		rank = 12d;
            	}
            	mrMulRowValue += (Double.parseDouble(parts[1]) * rank);
            }
            br.close();
        }
        
	}
	
	public void reduce(LongWritable key, Iterable<Cell> matrixCells,
            Context context) throws IOException, InterruptedException {
		
		Double newPageRank = 0d;
		
		for(Cell cell : matrixCells){
			Double prValue = pageRank.get(cell.getColumn());
			if(prValue != null){
				newPageRank += cell.getContribution() * prValue;
			}else{
				newPageRank += cell.getContribution() * pageRank.get(Constant.DUMMY_LONG_ID);
			}
		}
		udpatedPageRank.set(newPageRank);
		context.write(key, udpatedPageRank);
	}

	public void cleanup(Context context) throws IOException, InterruptedException{
		row.set(Constant.DUMMY_LONG_ID);
		udpatedPageRank.set((alpha/pageCount) + (1-alpha)*mrMulRowValue);
		context.write(row, udpatedPageRank);
	}
}
