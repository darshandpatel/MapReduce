package code;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Combiner {
	
	public static void main(String args[]) throws Exception{
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }
        conf.set("mapred.textoutputformat.separator", Constant.SEP);
        
		Job job = new Job(conf);
		job.setJarByClass(Combiner.class);
		
		job.setMapperClass(TokenizerMapper.class);
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

class MinMaxTempCombiner extends Reducer<Text, Text, Text, Text>{

	private Text resultInfo = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
	
		float sumTMAX = 0.0f;
		int countTMAX = 0;
		float sumTMIN = 0.0f;
		int countTMIN = 0;
		
		for(Text value : values){
			
			String data = value.toString();
			String parts[] = data.split(Constant.SEP);
			Float tmax = Float.parseFloat(parts[0]);
			Float tmaxC = Float.parseFloat(parts[1]);
			Float tmin = Float.parseFloat(parts[2]);
			Float tminC = Float.parseFloat(parts[3]);
			
			sumTMAX += tmax;
			countTMAX+= tmaxC;
			sumTMIN += tmin;
			countTMIN += tminC;
		}
		resultInfo.set(sumTMAX+Constant.SEP+countTMAX+Constant.SEP+sumTMIN+Constant.SEP+countTMIN);
		context.write(key, resultInfo);
	}
}
		