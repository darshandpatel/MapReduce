package main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
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
        // Read Page Id and Its Rank
        for (int i=0;i<status.length;i++){
        	Path path = status[i].getPath();
        	System.out.println("Check path :" + path.toString());
        	if(!path.toString().contains(".") && ! path.toString().contains("_SUCCESS")){
        		System.out.println("Okay path :" + path.toString());
	            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
	            String line;
	            while ((line = br.readLine()) != null){
	            	
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
        }
        
        System.out.println("****** PageRank Map size : "+pageRank.size());
        
        status = fs.listStatus(new Path(cacheFiles[1]));
        // Read dangling node page Id
        int danglingNodes = 0;
        for (int i=0;i<status.length;i++){
        	Path path = status[i].getPath();
        	System.out.println("Check 2nd path :" + path.toString());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            while ((line = br.readLine()) != null){
            	String[] parts = line.split("\t");
            	Long danglingPageId = Long.parseLong(parts[1]);
            	Double rank = pageRank.get(danglingPageId);
            	if(rank == null){
            		rank = pageRank.get(Constant.DUMMY_LONG_ID);
            	}
            	mrMulRowValue += ((1d/pageCount * rank));
            	//danglingNodePageRankSum += rank;
            	danglingNodes++;
            }
            br.close();
        }
        
        System.out.println("Dangling Nodes : "+danglingNodes);
        System.out.println("mrMulRowValue : "+mrMulRowValue);
        
	}
	
	public void reduce(LongWritable key, Iterable<Cell> values,
            Context context) throws IOException, InterruptedException {
		
		Double newPageRank = 0d;
		Double pageRankContrSum = 0d;
		int length = 0;
		//System.out.print("pageRankContrSum:");
		for(Cell cell : values){
			Double prValue = pageRank.get(cell.getIndex());
			if(prValue != null){
				pageRankContrSum += (cell.getContribution() * prValue);
			}else{
				pageRankContrSum += (cell.getContribution() * pageRank.get(Constant.DUMMY_LONG_ID));
			}
			length++;
			//System.out.print(pageRankContrSum+":");
		}
		//System.out.println("");
		newPageRank = ((alpha/pageCount) + (1-alpha)*(pageRankContrSum + mrMulRowValue));
		udpatedPageRank.set(newPageRank);
		//System.out.println("row id: "+ key + " Adj list :"+length + " Page Rank Contrib Sum : "+
		//pageRankContrSum + " alpha : "+alpha + " newPageRank :"+newPageRank + " mrMulRowValue : "+mrMulRowValue);
		context.write(key, udpatedPageRank);
	}

	public void cleanup(Context context) throws IOException, InterruptedException{
		row.set(Constant.DUMMY_LONG_ID);
		udpatedPageRank.set((alpha/pageCount) + (1-alpha)*(mrMulRowValue));
		context.write(row, udpatedPageRank);
	}
}
