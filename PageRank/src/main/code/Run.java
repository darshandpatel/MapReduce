package code;

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

        // Now cacluate the number of pages and dangling page/nodes in the parsed data
        Job numberJob = performNumberCalJob(Constant.PARSING_OUTPUT, Constant.NUMBER_OUTPUT, conf);
        // Example :
        // A : [B, C , D]
        // B : [C , A]
        // C : [D]
        // Here Total number of nodes are 4 (A B C D), Number of node could have be calcuated in the parsing job
        // but in parsing job we woundn't be able to count D as a node as its only exist in adjacency list.
        // The main pupose of number job to calculate all nodes in the graph including node such as D.

        Counter pageCounter = numberJob.getCounters().findCounter(COUNTERS.PAGE_COUNTER);
        conf.setLong(Constant.PAGE_COUNT, pageCounter.getValue());

        Counter danglingNodeCounter = numberJob.getCounters().findCounter(COUNTERS.NUMBER_OF_DANGLING_NODE);
        conf.setLong(Constant.NUMBER_OF_DANGLING_NODE, danglingNodeCounter.getValue());

        System.out.println("Page Counter : " + pageCounter.getValue());
        System.out.println("Number of Dangling node : " + danglingNodeCounter.getValue());

        conf.setDouble(Constant.ALPHA, 0.15);
        // Calculate the sum of dangling node page rank
        // conf.setDouble(Constant.DANGLING_NODES_PR_SUM, ((double)danglingNodeCounter.getValue())*(1d/pageCounter.getValue()));

        int iteration;
        // Page Rank Calculation Iteration
        for (iteration = 0; iteration <10; iteration++) {
            conf.setInt(Constant.ITERATION, iteration);
            String inputPath;
            inputPath = "data" + (iteration - 1);
            // First interation, source data is output of Number Calcuation Job.
            if (iteration == 0) {
                inputPath = Constant.NUMBER_OUTPUT;
            }

            Job pageRankJob = pageRankJob(inputPath, iteration, conf);

            Counter danglingNodesPRSum = pageRankJob.getCounters().findCounter(COUNTERS.DANGLING_NODE_PR_SUM);
            conf.setLong(Constant.DANGLING_NODES_PR_SUM, danglingNodesPRSum.getValue());

            Counter totalPR = pageRankJob.getCounters().findCounter(COUNTERS.TOTAL_PR);
            System.out.println("Total page rank : " + (totalPR.getValue()/Math.pow(10, Constant.POWER)));
            //System.out.println("Dangling Node Page Rank sum : " + (danglingNodesPRSum.getValue()/Math.pow(10, Constant.POWER)));

        }

        // This job calculates the top 100 records by pagerank
        Job top100 = Run.top100("data" + (iteration - 1), otherArgs[1], conf);
        //Counter totalPR = top100.getCounters().findCounter(COUNTERS.TOTAL_PR);

        // To display readable output
        //Run.sampleOutput("data"+(iteration-1), otherArgs[1], conf);
        // Final Total page rank of all pages in the graph.
        //System.out.println("Total page rank : " + (totalPR.getValue()/Math.pow(10, Constant.POWER)));


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


    /**
     * This method performs the map reduce job to calculate total number of nodes in the graph. This could have be
     * done in parsing job but to accomodate special dead node cases as we can not ignore them, this job is created.
     * @param inputPath
     * @param outputPath
     * @param conf
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static Job performNumberCalJob(String inputPath, String outputPath,
                                        Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(conf, "Number of Page/Dangling Node Job");
        job.setJarByClass(Run.class);
        job.setMapperClass(NumberMapper.class);
        job.setReducerClass(NumberReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setPartitionerClass(PageRankPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
        return job;

    }

    /**
     * This method calculates the page rank
     * @param inputPath
     * @param interation
     * @param conf
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static Job pageRankJob(String inputPath, int interation,
                                  Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(conf, "Page Rank " + interation);
        job.setJarByClass(Run.class);
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Node.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);
        job.setPartitionerClass(PageRankPartitioner.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path("data" + interation));

        job.waitForCompletion(true);
        return job;
    }


    /**
     * This method calculates the top 100 page with higher page rank.
     * @param inputPath
     * @param outputPath
     * @param conf
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static Job top100(String inputPath, String outputPath,
                             Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(conf, "Top 100");
        job.setJarByClass(Run.class);
        job.setMapperClass(TopMapper.class);
        job.setReducerClass(TopReducer.class);
        //job.setReducerClass(SampleReducer.class);
        job.setPartitionerClass(PageRankPartitioner.class);
        job.setSortComparatorClass(DoubleComparator.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(1);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
        return job;
    }

    /**
     * This method run the job by which output data can be seen
     * @param inputPath
     * @param outputPath
     * @param conf
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static Job sampleOutput(String inputPath, String outputPath,
                                   Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(conf, "Sample Output");
        job.setJarByClass(Run.class);
        job.setMapperClass(SampleMapper.class);
        job.setReducerClass(Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
        return job;
    }
}

