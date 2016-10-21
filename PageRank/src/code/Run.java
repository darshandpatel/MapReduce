package code;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Run {
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }
        
        Job parsingJob = performParsingJob(otherArgs, conf);
        Counter pageCounter = parsingJob.getCounters().findCounter(COUNTERS.PAGE_COUNTER);
        Counter danglingNodeCounter = parsingJob.getCounters().findCounter(COUNTERS.DANGLING_NODE_COUNTER);
        
        conf.setLong("pageCount", pageCounter.getValue());
        
        System.out.println("Page Counter : " + pageCounter.getValue());
        System.out.println("Dangling Node Counter : "+ danglingNodeCounter.getValue());
        
        for(int iteration = 0; iteration < 10; iteration++){
        	conf.setInt("iteraiton", iteration);
        	String inputPath = otherArgs[1];
        	if(iteration != 0){
        		inputPath = "data"+(iteration-1);
        	}
        	
        	Job pageRankJob = pageRankJob(inputPath, iteration, conf);
        }
        
        
		
	}
	
	public static Job performParsingJob(String[] otherArgs, Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{
		
		Job job = new Job(conf, "Job");
        job.setJarByClass(Run.class);
        job.setMapperClass(ParserMapper.class);
        job.setReducerClass(ParserReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Node.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        job.waitForCompletion(true);
		return job;
		
	}
	
	public static Job pageRankJob(String inputPath, int interation,
			Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{
		
		Job job = new Job(conf, "Job");
        job.setJarByClass(Run.class);
        job.setMapperClass(ParserMapper.class);
        job.setReducerClass(ParserReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Node.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path("data"+interation));
        
        job.waitForCompletion(true);
		return job;
		
	}

}

enum COUNTERS {
	PAGE_COUNTER,
	DANGLING_NODE_COUNTER
}
