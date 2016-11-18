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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Run {

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
        String outputParentFolder = otherArgs[1].substring(0, lastIndex+1);
        
        Job parsingJob = performParsingJob(otherArgs[0], outputParentFolder+Constant.PARSING_OUTPUT, conf);
        
        Counter pageCounter = parsingJob.getCounters().findCounter(COUNTERS.PAGE_COUNTER);
        conf.setLong(Constant.PAGE_COUNT, pageCounter.getValue());
        
        System.out.println("Number of pages : "+pageCounter.getValue());
        
        Job assignIdJob = assignIdToPages(outputParentFolder+Constant.PARSING_OUTPUT, 
        		Constant.TMP_DIR+Constant.ID_OUTPUT, conf);
        Job matrixBuildJob = buildMatrix(outputParentFolder+Constant.PARSING_OUTPUT, 
        		outputParentFolder+Constant.MATRIX_OUTPUT, outputParentFolder, conf);
        
        conf.setInt(Constant.ITERATION, 1);
        conf.setDouble(Constant.ALPHA, 0.15d);
        int iteration;
        for (iteration = 1; iteration <= 10; iteration++) {
            conf.setInt(Constant.ITERATION, iteration);
            String inputPath;
            
            inputPath = "data" + (iteration - 1);
            // First interation, source data is output of Number Calcuation Job.
            if (iteration == 1) {
                inputPath = Constant.NUMBER_OUTPUT;
            }

            Job pageRankJob = pageRankIteration(outputParentFolder+Constant.MATRIX_OUTPUT, Constant.TMP_DIR+Constant.DATA+iteration, 
            		outputParentFolder, iteration, conf);

        }
        
        Job top100 = Run.top100(Constant.TMP_DIR+Constant.DATA+(iteration-1), otherArgs[1], conf);
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
        job.setJarByClass(Run.class);
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
		job.setJarByClass(Run.class);
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
		job.setJarByClass(Run.class);
		job.setMapperClass(RowMatrixBuildMapper.class);
		job.setReducerClass(Reducer.class);
		
		//job.setMapOutputKeyClass(LongWritable.class);
		//job.setMapOutputValueClass(Cell.class);
		
		job.setOutputKeyClass(LongWritable.class);
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
    
    public static Job pageRankIteration(String inputPath, String outputPath, String outputParentFolder, int iteration,
            Configuration conf) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

		Job job = Job.getInstance(conf, "Parsing Job");
		job.setJarByClass(Run.class);
		job.setMapperClass(Mapper.class);
		job.setReducerClass(RowMatrixMulReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Cell.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		if(iteration == 1){
			Path path = new Path(Constant.TMP_DIR+Constant.ID_OUTPUT);
			URI cacheFile = new URI(path.toString()+"/"+Constant.IDS_MO+"-r-00000");
			job.addCacheFile(cacheFile);
		}else{
			Path path = new Path(Constant.TMP_DIR+Constant.DATA+(iteration-1));
			job.addCacheFile(path.toUri());
		}
		Path danglingNodePath = new Path(Constant.TMP_DIR+Constant.ID_OUTPUT);
		URI danglingNodeFile = new URI(danglingNodePath.toString()+"/"+Constant.DANGLING_MO+"-r-00000");
		job.addCacheFile(danglingNodeFile);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
		return job;
		
	}
    
    public static Job top100(String inputPath, String outputPath,
            Configuration conf) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

		Job job = Job.getInstance(conf, "Top 100");
		job.setJarByClass(Run.class);
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
}