package code;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.mortbay.log.Log;

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
        
		Job job = new Job(conf);
		job.setJarByClass(NoCombiner.class);
		
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TempStatus.class);
		
		job.setReducerClass(MinMaxTempReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
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
	
	private Text stationID = new Text();
	private TempStatus tempStatus = new TempStatus();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		// Parse input data
		String parts[] = value.toString().split(Constant.SEP);
		String id = parts[0].trim();
		String type = parts[2].trim();
		String tempValue = parts[3].trim();
		
		// Ignore the missing data
		if(!id.equals("") && !type.equals("") && !tempValue.equals("")){
			
			if(type.equals(Constant.TMAX)){
				stationID.set(id);
				tempStatus.setTmax(Float.parseFloat(tempValue));
				tempStatus.setTmaxCount(1);
				tempStatus.setTminCount(0);
				context.write(stationID, tempStatus);
			}else if(type.equals(Constant.TMIN)){
				stationID.set(id);
				tempStatus.setTmin(Float.parseFloat(tempValue));
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

	private Text resultInfo = new Text();
	
	public void reduce(Text key, Iterable<TempStatus> values, Context context) throws IOException, InterruptedException{
		
		float sumTMAX = 0.0f;
		int countTMAX = 0;
		float sumTMIN = 0.0f;
		int countTMIN = 0;
		
		// Iterate over all collected temperature data
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
		
		// Calculate Average Temperature
		if(countTMAX == 0 && countTMIN != 0){
			float tmin = sumTMIN/countTMIN;
			resultInfo.set(Constant.SEP+tmin);
		}else if(countTMIN == 0 && countTMAX != 0){
			float tmax = sumTMAX/countTMAX;
			resultInfo.set(tmax+Constant.SEP);
		}else{
			float tmin = sumTMIN/countTMIN;
			float tmax = sumTMAX/countTMAX;
			resultInfo.set(tmax+Constant.SEP+tmin);
		}
		context.write(key, resultInfo);
	}
}
