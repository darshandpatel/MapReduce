package main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Run {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }

        // First perform the parsing job
        // Parsing Job parses the source data and converts into source and its adjacency list format
        Job parsingJob = performParsingJob(otherArgs[0], Constant.PARSING_OUTPUT, conf);
        
        Counter pageCounter = parsingJob.getCounters().findCounter(COUNTERS.PAGE_COUNTER);
        conf.setLong(Constant.PAGE_COUNT, pageCounter.getValue());
        
        System.out.println("Number of pages : "+pageCounter.getValue());

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
}