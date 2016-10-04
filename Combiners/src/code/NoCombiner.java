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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class NoCombiner {
	
	public static void main(String args[]) throws Exception{
		
		Logger.getRootLogger().setLevel(Level.WARN);
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }
        
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
		
		if(!id.equals("") && !type.equals("") && !date.equals("") && !tempValue.equals("")){
			
			String year = date.substring(0,4);
			if(type.equals(Constant.TMAX)){
				stationID.set(id);
				stationTempDate.set(year+"\t"+tempValue+" "+Constant.NULL);
				context.write(stationID, stationTempDate);
			}else if(type.equals(Constant.TMIN)){
				stationID.set(id);
				stationTempDate.set(year+"\t"+Constant.NULL+" "+tempValue);
				context.write(stationID, stationTempDate);
			}
			
		}
	}
}

class MinMaxTempReducer extends Reducer<Text, Text, Text, Text>{

	private Text resultInfo = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
		// Reduce group the data with each station ID
		// Now Group the data for each year
		HashMap<String, Float[]> yearHashMap = new HashMap<String, Float[]>(); 
		
		for(Text value : values){
			
			String data = value.toString();
			String parts[] = data.split("\t");
			String year = parts[0];
			
			if(!yearHashMap.containsKey(year)){
				Float[] array = {0.0f, 0.0f, 0.0f, 0.0f};
				yearHashMap.put(year, array);
			}
			
			String tempValues[] = parts[1].split(" ");
			if(tempValues[0].equals(Constant.NULL)){
				Float tempValue = Float.parseFloat(tempValues[1]);
				yearHashMap.get(year)[2] += tempValue;
				yearHashMap.get(year)[3] += 1;
			}else if(tempValues[1].equals(Constant.NULL)){
				Float tempValue = Float.parseFloat(tempValues[0]);
				yearHashMap.get(year)[0] += tempValue;
				yearHashMap.get(year)[1] += 1;
			}
			
		}
		
		Iterator<Map.Entry<String, Float[]>> iterator = yearHashMap.entrySet().iterator();
		
		while(iterator.hasNext()){
			
			Map.Entry<String, Float[]> pair = iterator.next();
			
			Float[] results = pair.getValue();
			
			float tmax= results[0]/results[1];
			float tmin = results[2]/results[3];
			
			resultInfo = new Text(pair.getKey()+" "+tmax+ " "+tmin);
			context.write(key, resultInfo);
		}
	}
}
