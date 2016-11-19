package main;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import main.Constant;

public class ColumnMajorRun {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }

        // First perform the parsing job
        // Parsing Job parses the source data and converts into source and its adjacency list format
        int lastIndex = otherArgs[1].lastIndexOf("/");
        String cacheFolder = otherArgs[2];
        
        String outputParentFolder = otherArgs[1].substring(0, lastIndex+1);
        
        Job parsingJob = performParsingJob(otherArgs[0], outputParentFolder+Constant.PARSING_OUTPUT, conf);
        
        Counter pageCounter = parsingJob.getCounters().findCounter(COUNTERS.PAGE_COUNTER);
        conf.setLong(Constant.PAGE_COUNT, pageCounter.getValue());
        
        Counter nbrOfDanglingNode = parsingJob.getCounters().findCounter(COUNTERS.NUMBER_OF_DANGLING_NODE);
        conf.setLong(Constant.NUMBER_OF_DANGLING_NODE, nbrOfDanglingNode.getValue());
        
        System.out.println("Number of pages : "+pageCounter.getValue());
        System.out.println("Number of dangling pages : "+nbrOfDanglingNode.getValue());
        
        
        Job assignIdJob = assignIdToPages(outputParentFolder+Constant.PARSING_OUTPUT, 
        		cacheFolder+"/"+Constant.ID_OUTPUT, conf);
        
        Job matrixBuildJob = buildMatrix(outputParentFolder+Constant.PARSING_OUTPUT, 
        		outputParentFolder+Constant.MATRIX_OUTPUT, outputParentFolder, conf);
        
        conf.setInt(Constant.ITERATION, 1);
        conf.setDouble(Constant.ALPHA, 0.15d);
        
       //printJob(outputParentFolder+Constant.MATRIX_OUTPUT, Constant.TMP_DIR+Constant.DATA+"print", conf);
        
        
        int iteration;
        for (iteration = 1; iteration <= 1; iteration++) {
            conf.setInt(Constant.ITERATION, iteration);
            Job pageRankJob = pageRankIteration(outputParentFolder+Constant.MATRIX_OUTPUT, 
            		cacheFolder+"/"+Constant.DATA+iteration, 
            		cacheFolder+"/", iteration, conf);
        }
        
        Job top100 = ColumnMajorRun.top100(Constant.TMP_DIR+Constant.DATA+(iteration-1), otherArgs[1], conf);
        
    }

    /**
     * This method performs the parsin Map Reduce job.
     * @param inputPath
     * @param outputPath
     * @param conf
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static Job performParsingJob(String inputPath, String outputPath,
                                        Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(conf, "Parsing Job");
        job.setJarByClass(ColumnMajorRun.class);
        job.setMapperClass(ParserMapper.class);
        job.setReducerClass(ParserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setPartitionerClass(PageRankPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
        return job;

    }
    
    public static Job assignIdToPages(String inputPath, String outputPath,
            Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

		Job job = Job.getInstance(conf, "Parsing Job");
		job.setJarByClass(ColumnMajorRun.class);
		job.setMapperClass(IdMapper.class);
		job.setReducerClass(IdReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Node.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		
		MultipleOutputs.addNamedOutput(job, Constant.DANGLING_MO, TextOutputFormat.class,
			    Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, Constant.IDS_MO, TextOutputFormat.class,
			    Text.class, Text.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
		return job;
		
	}
    
    public static Job buildMatrix(String inputPath, String outputPath, String outputParentFolder,
            Configuration conf) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

		Job job = Job.getInstance(conf, "Parsing Job");
		job.setJarByClass(ColumnMajorRun.class);
		job.setMapperClass(ColumnMatrixBuildMapper.class);
		job.setReducerClass(Reducer.class);
		job.setMapOutputKeyClass(Cell.class);
		job.setMapOutputValueClass(Cell.class);
		job.setOutputKeyClass(Cell.class);
		job.setOutputValueClass(Cell.class);
		
		Path path = new Path(Constant.TMP_DIR+Constant.ID_OUTPUT);
		URI cacheFile = new URI(path.toString()+"/"+Constant.IDS_MO+"-r-00000");
		job.addCacheFile(cacheFile);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
		return job;
		
	}
    
    public static Job pageRankIteration(String inputPath, String outputPath, String cacheFolder, int iteration,
            Configuration conf) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

		Job job = Job.getInstance(conf, "Iteration Job");
		job.setJarByClass(ColumnMajorRun.class);
		job.setReducerClass(ColumnMatrixMulReducer.class);
		job.setMapOutputKeyClass(Cell.class);
		job.setMapOutputValueClass(Cell.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		Path rankFilepath;
		if(iteration == 1){
			rankFilepath= new Path(cacheFolder+"/"+Constant.ID_OUTPUT+"/"+Constant.IDS_MO+"-r-00000");
		}else{
			rankFilepath = new Path(cacheFolder+"/"+Constant.DATA+(iteration-1));
		}
		
		MultipleInputs.addInputPath(job, rankFilepath, TextInputFormat.class, ColumnRankMatrixMapper.class);
		MultipleInputs.addInputPath(job, new Path(inputPath), SequenceFileInputFormat.class, Mapper.class);
		 
		FileOutputFormat.setOutputPath(job, new Path(outputPath+"temp"));
		
		job.waitForCompletion(true);
		
		Job sumJob = Job.getInstance(conf, "Sum pagerank contribution");
		sumJob.setJarByClass(ColumnMajorRun.class);
		sumJob.setMapperClass(Mapper.class);
		sumJob.setReducerClass(ColumnMatrixMulSumReducer.class);
		sumJob.setMapOutputKeyClass(LongWritable.class);
		sumJob.setMapOutputValueClass(DoubleWritable.class);
		sumJob.setOutputKeyClass(LongWritable.class);
		sumJob.setOutputValueClass(DoubleWritable.class);
		sumJob.setInputFormatClass(SequenceFileInputFormat.class);
		
		if(iteration == 1){
			Path path = new Path(cacheFolder+"/"+Constant.ID_OUTPUT);
			URI cacheFile = new URI(path.toString()+"/"+Constant.IDS_MO+"-r-00000");
			sumJob.addCacheFile(cacheFile);
		}else{
			Path path = new Path(cacheFolder+"/"+Constant.DATA+(iteration-1));
			sumJob.addCacheFile(path.toUri());
		}
		Path danglingNodePath = new Path(cacheFolder+"/"+Constant.ID_OUTPUT);
		URI danglingNodeFile = new URI(danglingNodePath.toString()+"/"+Constant.DANGLING_MO+"-r-00000");
		sumJob.addCacheFile(danglingNodeFile);
		
		FileInputFormat.addInputPath(sumJob, new Path(outputPath+"temp"));
		FileOutputFormat.setOutputPath(sumJob, new Path(outputPath));
		
		sumJob.waitForCompletion(true);
		return sumJob;
		
		//return null;
		
	}
    
    public static Job top100(String inputPath, String outputPath,
            Configuration conf) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

		Job job = Job.getInstance(conf, "Top 100");
		job.setJarByClass(ColumnMajorRun.class);
		job.setMapperClass(TopMapper.class);
		//job.setMapperClass(SampleMapper.class);
		job.setReducerClass(TopReducer.class);
		//job.setReducerClass(SampleReducer.class);
		job.setSortComparatorClass(DoubleComparator.class);
		
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		Path path = new Path(Constant.TMP_DIR+Constant.ID_OUTPUT);
		URI cacheFile = new URI(path.toString()+"/"+Constant.IDS_MO+"-r-00000");
		job.addCacheFile(cacheFile);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setNumReduceTasks(1);
		//job.setInputFormatClass(SequenceFileInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
		return job;
	}
    
    public static Job printJob(String inputPath, String outputPath,
            Configuration conf) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

		Job job = Job.getInstance(conf, "Print Job");
		job.setJarByClass(ColumnMajorRun.class);
		job.setMapperClass(PrintRowMatrixBuildMapper.class);
		job.setReducerClass(Reducer.class);
		//job.setReducerClass(SampleReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
		return job;
	}
    
    
}