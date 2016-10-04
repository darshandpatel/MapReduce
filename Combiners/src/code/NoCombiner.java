package code;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * NoCombiner class calls a map reduce job without combiner 
 * @author Darshan
 *
 */
public class NoCombiner {
	
	public static void main(String args[]) throws Exception{
		
		Logger.getRootLogger().setLevel(Level.WARN);
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }
        conf.set("mapred.textoutputformat.separator", ",");
        
		Job job = new Job();
		job.setJarByClass(NoCombiner.class);
		job.setMapperClass(TokenizerMapper.class);
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

class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
	
	private Text stationID = new Text();
	private Text stationTempDate = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		String parts[] = value.toString().split(",");
		String id = parts[0].trim();
		String date = parts[1].trim();
		String type = parts[2].trim();
		String tempValue = parts[3].trim();
		
		// Ignore the missing data 
		if(!id.equals("") && !type.equals("") && !date.equals("") && !tempValue.equals("")){

			if(type.equals(Constant.TMAX)){
				stationID.set(id);
				// Value format is "TMAX\tTMIN"
				stationTempDate.set(tempValue+"\t"+Constant.NULL);
				context.write(stationID, stationTempDate);
			}else if(type.equals(Constant.TMIN)){
				stationID.set(id);
				//Value format is "TMAX\tTMIN"
				stationTempDate.set(Constant.NULL+"\t"+tempValue);
				context.write(stationID, stationTempDate);
			}
			
		}
	}
}

class MinMaxTempReducer extends Reducer<Text, Text, Text, Text>{

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
		// results = {sum Of TMAX, count of TMAX, sum of TMIN, count of TMIN}
		Float[] results =  {0.0f, 0.0f, 0.0f, 0.0f};
		Text resultInfo = new Text();
		for(Text value : values){
			
			String data = value.toString();
			String parts[] = data.split("\t");
			String tmaxValue = parts[0];
			String tminValue = parts[1];
			
			if(tmaxValue.equals(Constant.NULL)){
				Float tempValue = Float.parseFloat(tminValue);
				results[2] += tempValue;
				results[3] += 1;
			}else if(tminValue.equals(Constant.NULL)){
				Float tempValue = Float.parseFloat(tmaxValue);
				results[0] += tempValue;
				results[1] += 1;
			}
		}
		
		float tmax= results[0]/results[1];
		float tmin = results[2]/results[3];
		
		resultInfo = new Text(tmax+","+tmin);
		context.write(key, resultInfo);
	}
}
