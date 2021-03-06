package code;

import java.io.IOException;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * NoCombiner class calls a map reduce job without combiner 
 * @author Darshan
 *
 */
public class NoCombiner {
	
	public static void main(String args[]) throws Exception{
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }
        // Configure to have comma separator between reducer produced key and value in output file 
        conf.set("mapred.textoutputformat.separator", Constant.SEP);
        
        // MapReduce Job configuration
		Job job = new Job(conf);
		job.setJarByClass(NoCombiner.class);
		
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TempStatus.class);
		
		job.setReducerClass(MinMaxTempReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// MapReduce Job Input 
		for (int i = 0; i < otherArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
		
		// MapReduce Job Output
		FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
		job.waitForCompletion(true);
	}

}

/** 
 * Mapper class
 * @author Darshan
 *
 */
class TokenizerMapper extends Mapper<Object, Text, Text, TempStatus>{
//class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
	
	private Text stationID = new Text();
	private TempStatus tempStatus = new TempStatus();
	private static final Log LOG = LogFactory.getLog(TokenizerMapper.class);
	private int mapCallCount;
	
	public void setup(Context context) throws IOException, InterruptedException{
		LOG.info("Map is called");
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		// Parse input data
		String parts[] = value.toString().split(Constant.SEP);
		String id = parts[0].trim();
		String type = parts[2].trim();
		String tempValue = parts[3].trim();
		
		// Ignore the missing data
		if(!id.equals("") && !type.equals("") && !tempValue.equals("")){
			
			// Extract TMIN and TMAX value
			if(type.equals(Constant.TMAX)){
				stationID.set(id);
				
				// Set TMAX/TMIN value and count in Writable class 
				tempStatus.setTmin(0);
				tempStatus.setTmax(Float.parseFloat(tempValue));
				tempStatus.setTminCount(0);
				tempStatus.setTmaxCount(1);
				
				context.write(stationID, tempStatus);
			}else if(type.equals(Constant.TMIN)){
				stationID.set(id);
				
				// Set TMAX/TMIN value and count in Writable class 
				tempStatus.setTmin(Float.parseFloat(tempValue));
				tempStatus.setTmax(0);
				tempStatus.setTminCount(1);
				tempStatus.setTmaxCount(0);
				
				context.write(stationID, tempStatus);
			}
		}
	}
	
}

/**
 * Reducer class
 * @author Darshan
 *
 */
class MinMaxTempReducer extends Reducer<Text, TempStatus, Text, Text>{
//class MinMaxTempReducer extends Reducer<Text, Text, Text, Text>{

	private Text resultInfo = new Text();
	
	public void reduce(Text key, Iterable<TempStatus> values, Context context) throws IOException, InterruptedException{
		
		float sumTMAX = 0.0f;
		long countTMAX = 0l;
		float sumTMIN = 0.0f;
		long countTMIN = 0l;
		
		// Iterate over all collected temperature data and sum the TMIN/TMAX value and count
		for(TempStatus value : values){
			
			if(value.getTmaxCount() >= 1){
				sumTMAX += value.getTmax(); 
				countTMAX += value.getTmaxCount();
			}
			if(value.getTminCount() >= 1){
				sumTMIN += value.getTmin();
				countTMIN += value.getTminCount();
			}
		}
		
		// Calculate TMIN and TMAX average
		if(countTMAX != 0 && countTMIN != 0){
			resultInfo.set(sumTMIN/countTMIN+Constant.SEP+sumTMAX/countTMAX);
		}else if(countTMAX != 0){
			resultInfo.set(Constant.SEP+sumTMAX/countTMAX);
		}else if(countTMIN != 0){
			resultInfo.set(sumTMIN/countTMIN+Constant.SEP);
		}
		
		context.write(key, resultInfo);
	}
}
