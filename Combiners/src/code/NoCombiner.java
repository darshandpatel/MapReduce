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
        conf.set("mapred.textoutputformat.separator", Constant.SEP);
        
		Job job = new Job(conf);
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
	private Text stationTemp = new Text();
	
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
				// Value format is "TMAX,TMIN"
				stationTemp.set(tempValue+Constant.SEP+"1"+Constant.SEP+"0"+Constant.SEP+"0");
				context.write(stationID, stationTemp);
			}else if(type.equals(Constant.TMIN)){
				stationID.set(id);
				//Value format is "TMAX\tTMIN"
				stationTemp.set("0"+Constant.SEP+"0"+Constant.SEP+tempValue+Constant.SEP+"1");
				context.write(stationID, stationTemp);
			}
			
		}
	}
}

class MinMaxTempReducer extends Reducer<Text, Text, Text, Text>{

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
			
		if(countTMAX == 0){
			float tmin = sumTMIN/countTMIN;
			resultInfo = new Text(Constant.NULL+Constant.SEP+tmin);
		}else if(countTMIN == 0){
			float tmax = sumTMAX/countTMAX;
			resultInfo = new Text(tmax+Constant.SEP+Constant.NULL);
		}else{
			float tmin = sumTMIN/countTMIN;
			float tmax = sumTMAX/countTMAX;
			resultInfo = new Text(tmax+Constant.SEP+tmin);
		}
		context.write(key, resultInfo);
		
	}
}