package code;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class InMapperCombiner {
	
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
		
		job.setMapperClass(MapperWithCombiner.class);
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

class MapperWithCombiner extends Mapper<Object, Text, Text, Text>{
	
	private Text stationID = new Text();
	private Text stationTemp = new Text();
	private HashMap<String, Info> summary;
	
	protected void setup(Context context) throws IOException, InterruptedException{
		summary = new HashMap<String, Info>();
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		String parts[] = value.toString().split(Constant.SEP);
		String id = parts[0].trim();
		String type = parts[2].trim();
		String tempValue = parts[3].trim();
		
		// Ignore the missing data 
		if(!id.equals("") && !type.equals("") && !tempValue.equals("")){
			
			if(summary.containsKey(id)){
				if(type.equals(Constant.TMAX)){
					summary.get(id).addTmax(Float.parseFloat(tempValue));
				}else if(type.equals(Constant.TMIN)){
					summary.get(id).addTmin(Float.parseFloat(tempValue));
				}
			}else{
				if(type.equals(Constant.TMAX)){
					Info info = new Info(0, 0, Float.parseFloat(tempValue), 1);
					summary.put(id, info);
				}else if(type.equals(Constant.TMIN)){
					Info info = new Info(Float.parseFloat(tempValue), 1, 0, 0);
					summary.put(id, info);
				}
			}
			
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		
		Iterator<Map.Entry<String, Info>> iterator = summary.entrySet().iterator();
		
		while(iterator.hasNext()){
			
			Map.Entry<String, Info> pair = iterator.next();
			stationID.set(pair.getKey());
			Info info = pair.getValue();
			stationTemp.set(info.getTmaxSum()+Constant.SEP+info.getTmaxCount()+
					Constant.SEP+info.getTminSum()+Constant.SEP+info.getTminCount());
			context.write(stationID, stationTemp);
			
		}
	}
}
