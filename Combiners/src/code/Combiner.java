package code;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.PropertyConfigurator;


public class Combiner {
	
	public static void main(String args[]) throws Exception{
		
		//String log4jConfPath = "/usr/local/hadoop/etc/hadoop/log4j.properties";
    	//PropertyConfigurator.configure(log4jConfPath);
    	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }
        
        //Configure to have comma separator between reducer produced key and value in output file
        conf.set("mapred.textoutputformat.separator", Constant.SEP);
        
		Job job = new Job(conf);
		job.setJarByClass(Combiner.class);
		
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TempStatus.class);
		
		job.setCombinerClass(MinMaxTempCombiner.class);
		job.setReducerClass(MinMaxTempReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
		FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

/**
 * Combiner class
 * @author Darshan
 *
 */
class MinMaxTempCombiner extends Reducer<Text, TempStatus, Text, TempStatus>{

	private TempStatus result = new TempStatus();
	
	public void reduce(Text key, Iterable<TempStatus> values, Context context) throws IOException, InterruptedException{
	
		float sumTMAX = 0.0f;
		int countTMAX = 0;
		float sumTMIN = 0.0f;
		int countTMIN = 0;
		
		// Iterate over data generated by Mapper
		for(TempStatus value : values){
			if(value.getTmaxCount() == 1){
				sumTMAX += value.getTmax(); 
				countTMAX++;
			}else{
				sumTMIN += value.getTmin();
				countTMIN++;
			}
		}
		
		// Aggregate the temperature data
		if(countTMAX == 0 && countTMIN != 0){
			float tmin = sumTMIN/countTMIN;
			result.setTmin(tmin);
			result.setTminCount(countTMIN);
		}else if(countTMIN == 0 && countTMAX != 0){
			float tmax = sumTMAX/countTMAX;
			result.setTmax(tmax);
			result.setTmaxCount(countTMAX);
		}else{
			float tmin = sumTMIN/countTMIN;
			float tmax = sumTMAX/countTMAX;
			result.setTmax(tmax);
			result.setTmaxCount(countTMAX);
			result.setTmin(tmin);
			result.setTminCount(countTMIN);
		}
		context.write(key, result);
		
	}
}
		