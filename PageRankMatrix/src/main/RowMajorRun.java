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

public class RowMajorRun {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }

        // First perform the parsing job
        // Parsing Job parses the source data and converts into source and its adjacency list format
        System.out.println("Input Folder Path : " + otherArgs[0]);
        int lastIndex = otherArgs[1].lastIndexOf("/");
        System.out.println("Cache Folder : " + otherArgs[2]);
        System.out.println("Output Folder : " + otherArgs[1]);
        String cacheFolder = otherArgs[2];
        
        String outputParentFolder = otherArgs[1].substring(0, lastIndex+1);
        
        Job parsingJob = performParsingJob(otherArgs[0]+"/", outputParentFolder+Constant.PARSING_OUTPUT+"/", conf);
        
        Counter pageCounter = parsingJob.getCounters().findCounter(COUNTERS.PAGE_COUNTER);
        conf.setLong(Constant.PAGE_COUNT, pageCounter.getValue());
        
        Counter nbrOfDanglingNode = parsingJob.getCounters().findCounter(COUNTERS.NUMBER_OF_DANGLING_NODE);
        conf.setLong(Constant.NUMBER_OF_DANGLING_NODE, nbrOfDanglingNode.getValue());
        
        System.out.println("Number of pages : "+pageCounter.getValue());
        System.out.println("Number of dangling pages : "+nbrOfDanglingNode.getValue());
        
        Job assignIdJob = assignIdToPages(outputParentFolder+Constant.PARSING_OUTPUT+"/", 
        		cacheFolder+"/"+Constant.ID_OUTPUT+"/", conf);
        
        Job matrixBuildJob = buildMatrix(outputParentFolder+Constant.PARSING_OUTPUT+"/", 
        		outputParentFolder+Constant.MATRIX_OUTPUT+"/", cacheFolder, conf);
        
        conf.setDouble(Constant.ALPHA, 0.15d);
        
       //printJob(outputParentFolder+Constant.MATRIX_OUTPUT, Constant.TMP_DIR+Constant.DATA+"print", conf);
        
        int iteration;
        for (iteration = 1; iteration <= 10; iteration++) {
            conf.setInt(Constant.ITERATION, iteration);
            Job pageRankJob = pageRankIteration(outputParentFolder+Constant.MATRIX_OUTPUT+"/", 
            		cacheFolder+"/"+Constant.DATA+iteration, 
            		cacheFolder, iteration, conf);
        }
        
        Job top100 = ColumnMajorRun.top100(cacheFolder+"/"+Constant.DATA+(iteration-1)+"/", otherArgs[1]+"/", 
        		cacheFolder, conf);
        		
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
        job.setJarByClass(RowMajorRun.class);
        job.setMapperClass(ParserMapper.class);
        job.setReducerClass(ParserReducer.class);
        job.setCombinerClass(ParserCombiner.class);
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
		job.setJarByClass(RowMajorRun.class);
		job.setMapperClass(IdMapper.class);
		job.setReducerClass(Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Node.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(PageRankPartitioner.class);
		
		MultipleOutputs.addNamedOutput(job, Constant.DANGLING_MO, TextOutputFormat.class,
			    Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, Constant.IDS_MO, TextOutputFormat.class,
			    Text.class, Text.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
		return job;
		
	}
    
    public static Job buildMatrix(String inputPath, String outputPath, String cacheFolder,
            Configuration conf) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

		Job job = Job.getInstance(conf, "Parsing Job");
		job.setJarByClass(RowMajorRun.class);
		job.setMapperClass(RowMatrixBuildMapper.class);
		job.setReducerClass(Reducer.class);
		job.setMapOutputKeyClass(Cell.class);
		job.setMapOutputValueClass(Cell.class);
		job.setOutputKeyClass(Cell.class);
		job.setOutputValueClass(Cell.class);
		job.setPartitionerClass(RowColumnCellPartitioner.class);
		
		Path path = new Path(cacheFolder+"/"+Constant.ID_OUTPUT+"/"+Constant.IDS_MO+"/");
		job.addCacheFile(path.toUri());
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
		return job;
		
	}
    
    public static Job pageRankIteration(String inputPath, String outputPath, String cacheFolder, int iteration,
            Configuration conf) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

		Job job = Job.getInstance(conf, "Parsing Job");
		job.setJarByClass(RowMajorRun.class);
		//job.setMapperClass(Mapper.class);
		//job.setMapperClass(RowMatrixMulMapper.class);
		job.setReducerClass(RowMatrixMulReducer.class);
		
		job.setMapOutputKeyClass(Cell.class);
		job.setMapOutputValueClass(Cell.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
		job.setPartitionerClass(RowColumnCellPartitioner.class);
		
		if(iteration == 1){
			Path path = new Path(cacheFolder+"/"+Constant.ID_OUTPUT+"/"+Constant.IDS_MO+"/");
			job.addCacheFile(path.toUri());
		}else{
			Path path = new Path(cacheFolder+"/"+Constant.DATA+(iteration-1)+"/");
			job.addCacheFile(path.toUri());
		}
		Path danglingNodePath = new Path(cacheFolder+"/"+Constant.ID_OUTPUT+"/"+Constant.DANGLING_MO+"/");
		job.addCacheFile(danglingNodePath.toUri());
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
		return job;
		
	}
    
    public static Job top100(String inputPath, String outputPath, String cacheFolder,
            Configuration conf) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

		Job job = Job.getInstance(conf, "Top 100");
		job.setJarByClass(RowMajorRun.class);
		job.setMapperClass(TopMapper.class);
		//job.setMapperClass(SampleMapper.class);
		job.setReducerClass(TopReducer.class);
		//job.setReducerClass(SampleReducer.class);
		job.setSortComparatorClass(DoubleComparator.class);
		
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		Path path = new Path(cacheFolder+"/"+Constant.ID_OUTPUT+"/"+Constant.IDS_MO+"/");
		job.addCacheFile(path.toUri());
		
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
		job.setJarByClass(RowMajorRun.class);
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