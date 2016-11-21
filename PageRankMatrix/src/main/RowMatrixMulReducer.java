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

public class RowMatrixMulReducer extends Reducer<Cell, Cell, LongWritable, DoubleWritable> {
	
	HashMap<Long, Double> pageRank;
	Double mrMulRowValue;
	
	DoubleWritable udpatedPageRank = new DoubleWritable();
	LongWritable row = new LongWritable();
	int iteration;
	double alpha;
    long pageCount;
	FileSystem fs;
    
	public void setup(Context context) throws IOException{
		
		Configuration conf =  context.getConfiguration();
		pageCount = conf.getLong(Constant.PAGE_COUNT, -10);
		alpha = conf.getDouble(Constant.ALPHA, -10);
		iteration = conf.getInt(Constant.ITERATION, -10);
		
		pageRank = new HashMap<Long, Double>();
		mrMulRowValue = 0d;
		
        URI[] cacheFiles = context.getCacheFiles();
		Path sourceFilePath = new Path(cacheFiles[0]);
		fs = FileSystem.get(sourceFilePath.toUri(), conf);
		FileStatus[] status = fs.listStatus(sourceFilePath);
		
        // Read Page Id and Its Rank
        for (int i=0;i<status.length;i++){
        	Path path = status[i].getPath();
        	System.out.println("!!! Check path :" + path.toString());
        	if(!path.toString().contains(".") && ! path.toString().contains("_SUCCESS") && !path.toString().contains("crc")){
        		System.out.println("!!! Okay path :" + path.toString());
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
        System.out.println(" Dummy key value " + pageRank.get(Constant.DUMMY_LONG_ID));
        
        Path danglingNodePath = new Path(cacheFiles[1]);
		fs = FileSystem.get(danglingNodePath.toUri(), conf);
		status = fs.listStatus(danglingNodePath);
        //status = fs.listStatus(new Path(cacheFiles[1]));
        // Read dangling node page Id
        for (int i=0;i<status.length;i++){
        	Path path = status[i].getPath();
        	System.out.println("!!! Check path :" + path.toString());
        	if(!path.toString().contains(".") && ! path.toString().contains("_SUCCESS") && !path.toString().contains("crc")){
        		System.out.println("!!! Okay path :" + path.toString());
	            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
	            String line;
	            while ((line = br.readLine()) != null){
	            	String[] parts = line.split("\t");
	            	Long danglingPageId = Long.parseLong(parts[1]);
	            	Double rank;
	            	if(!pageRank.containsKey(danglingPageId)){
	            		rank = pageRank.get(Constant.DUMMY_LONG_ID);
	            	}else{
	            		rank = pageRank.get(danglingPageId);
	            	}
	            	mrMulRowValue += rank;
	            }
	            br.close();
	        }
        }
        mrMulRowValue = (1d/pageCount) * mrMulRowValue;
        System.out.println("mrMulRowValue : "+mrMulRowValue);
        
	}
	
	public void reduce(Cell key, Iterable<Cell> values,
            Context context) throws IOException, InterruptedException {
		
		Double newPageRank = 0d;
		Double pageRankContrSum = 0d;
		//System.out.print("pageRankContrSum:");
		
		boolean first = true;
		for(Cell cell : values){
			
			if(first){
				first = false;
			}else{
				Double prValue;
				if(!pageRank.containsKey(cell.getIndex())){
					prValue = pageRank.get(Constant.DUMMY_LONG_ID);
				}else{
					prValue = pageRank.get(cell.getIndex());
				}
				pageRankContrSum += (cell.getContribution() * prValue);
			}
		}
		//System.out.println("");
		newPageRank = ((alpha/pageCount) + (1-alpha)*(pageRankContrSum + mrMulRowValue));
		udpatedPageRank.set(newPageRank);
		//System.out.println("row id: "+ key + " Adj list :"+length + " Page Rank Contrib Sum : "+
		//pageRankContrSum + " alpha : "+alpha + " newPageRank :"+newPageRank + " mrMulRowValue : "+mrMulRowValue);
		row.set(key.getIndex());
		context.write(row, udpatedPageRank);
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		row.set(Constant.DUMMY_LONG_ID);
		udpatedPageRank.set((alpha/pageCount) + (1-alpha)*mrMulRowValue);
		context.write(row, udpatedPageRank);
	}

}
