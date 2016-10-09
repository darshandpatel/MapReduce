package code;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SecondarySort {
	
	public static void main(String args[]) throws Exception{
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }
        conf.set("mapred.textoutputformat.separator", Constant.SEP);
        
		Job job = new Job(conf);
		job.setJarByClass(SecondarySort.class);
		
		job.setMapperClass(SecondarySortMapper.class);
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(TempStatus.class);
		
		job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
		
		//job.setNumReduceTasks(5);
		job.setReducerClass(SecondarySortReducer.class);
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

class SecondarySortMapper extends Mapper<Object, Text, CompositeKey, TempStatus>{
	
	private CompositeKey compositeKey = new CompositeKey();
	private TempStatus tempStatus = new TempStatus();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		String parts[] = value.toString().split(",");
		String id = parts[0].trim();
		String date = parts[1].trim();
		String type = parts[2].trim();
		String tempValue = parts[3].trim();
		
		// Ignore the missing data 
		if(!id.equals("") && !type.equals("") && !date.equals("") && !tempValue.equals("")){

			int year = Integer.parseInt(date.substring(0, 4));
			compositeKey.setStationId(id);
			compositeKey.setYear(year);
			
			if(type.equals(Constant.TMAX)){
				
				tempStatus.setTmax(Float.parseFloat(tempValue));
				tempStatus.setTmin(0);
				tempStatus.setYear(year);
				tempStatus.setIsTmax(true);
				
				context.write(compositeKey, tempStatus);
				
			}else if(type.equals(Constant.TMIN)){
				
				tempStatus.setTmin(Float.parseFloat(tempValue));
				tempStatus.setTmax(0);
				tempStatus.setYear(year);
				tempStatus.setIsTmax(false);
				
				context.write(compositeKey, tempStatus);
			}
		
		}
	}
	
}


class SecondarySortReducer extends Reducer<CompositeKey, TempStatus, Text, Text>{
	
	private Text stationId = new Text();
	private Text resultStr = new Text();
	
	public void reduce(CompositeKey key, Iterable<TempStatus> values, 
			Context context) throws IOException, InterruptedException{
		
		StringBuilder result = new StringBuilder();
		result.append("[");
		
		int previousYear = Constant.RANDOM_NUM;
		float tmaxSum = 0;
		int tmaxCount = 0;
		float tminSum = 0;
		int tminCount = 0;
		boolean firstTime = true;
		
		for(TempStatus tempStatus : values){
			
			if(firstTime){
				previousYear = tempStatus.getYear();
				firstTime = false;
			}
			
			if(tempStatus.getYear() != previousYear){
				
				if(previousYear != Constant.RANDOM_NUM){
					
					if(tminCount != 0 && tmaxCount != 0){
						result.append("("+previousYear+","+(tminSum/tminCount)+","+(tmaxSum/tmaxCount)+")");
					}else if(tminCount != 0){
						result.append("("+previousYear+","+(tminSum/tminCount)+",)");
					}else if(tmaxCount != 0){
						result.append("("+previousYear+",,"+(tmaxSum/tmaxCount)+")");
					}
					
					previousYear = tempStatus.getYear();
					tmaxSum = tempStatus.getTmax();
					tminSum = tempStatus.getTmin();
					if(tempStatus.isTmax()){
						tminCount = 0;
						tmaxCount = 1;
					}else{
						tminCount = 1;
						tmaxCount = 0;
					}
				}
			}else{
				if(tempStatus.isTmax()){
					tmaxSum += tempStatus.getTmax();
					tmaxCount++;
				}else{
					tminSum += tempStatus.getTmin();
					tminCount++;
				}
			}
			
		}
		
		if(tminCount != 0 && tmaxCount != 0){
			result.append("("+previousYear+","+(tminSum/tminCount)+","+(tmaxSum/tmaxCount)+")]");
		}else if(tminCount != 0){
			result.append("("+previousYear+","+(tminSum/tminCount)+",)]");
		}else if(tmaxCount != 0){
			result.append("("+previousYear+",,"+(tmaxSum/tmaxCount)+")]");
		}
		stationId.set(key.getStationId());
		resultStr.set(result.toString());
		context.write(stationId, resultStr);
	}
	
}
