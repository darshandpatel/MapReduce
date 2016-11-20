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

public class ColumnMatrixMulSumReducer extends Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {
	
	DoubleWritable newPageRank = new DoubleWritable();
	
	Long pageCount;
	Double alpha;
	
	HashMap<Long, Double> pageRank;
	Double mrMulRowValue;
	
	DoubleWritable udpatedPageRank = new DoubleWritable();
	LongWritable row = new LongWritable();
	int iteration;
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
	            	Double rank = pageRank.get(danglingPageId);
	            	mrMulRowValue += rank;
	            }
	            br.close();
	        }
        }
        mrMulRowValue = (1d/pageCount) * mrMulRowValue;
        System.out.println("mrMulRowValue : "+mrMulRowValue);
	}
    
	public void reduce(LongWritable key, Iterable<DoubleWritable> values,
            Context context) throws IOException, InterruptedException {
		
		Double totalPRContribution = 0d;
		for(DoubleWritable prContribution : values){
			totalPRContribution += prContribution.get();
		}
		newPageRank.set(alpha/pageCount + (1-alpha)*(totalPRContribution+mrMulRowValue));
		context.write(key, newPageRank);
	}

}
