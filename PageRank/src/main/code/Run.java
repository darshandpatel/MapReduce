package code;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
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

        Job parsingJob = performParsingJob(otherArgs[0], Constant.PARSING_OUTPUT, conf);
        Job numberJob = performNumberCalJob(Constant.PARSING_OUTPUT, Constant.NUMBER_OUTPUT, conf);

        Counter pageCounter = numberJob.getCounters().findCounter(COUNTERS.PAGE_COUNTER);
        System.out.println("Page Counter : " + pageCounter.getValue());
        conf.setLong(Constant.PAGE_COUNT, pageCounter.getValue());

        Counter danglingNodeCounter = numberJob.getCounters().findCounter(COUNTERS.NUMBER_OF_DANGLING_NODE);
        System.out.println("Number of Dangling node : " + danglingNodeCounter.getValue());
        conf.setLong(Constant.NUMBER_OF_DANGLING_NODE, danglingNodeCounter.getValue());

        conf.setDouble(Constant.ALPHA, 0.15);

        /*
        int iteration;
        for (iteration = 0; iteration <20; iteration++) {
            conf.setInt("iteration", iteration);
            String inputPath;
            inputPath = "data" + (iteration - 1);
            if (iteration == 0) {
                inputPath = "parsing";
            }

            Job pageRankJob = pageRankJob(inputPath, iteration, conf);

            Counter danglingNodesPRSum = pageRankJob.getCounters().findCounter(COUNTERS.DANGLING_NODE_PR_SUM);
            conf.setLong(Constant.DANGLING_NODES_PR_SUM, danglingNodesPRSum.getValue());

            Counter totalPR = pageRankJob.getCounters().findCounter(COUNTERS.TOTAL_PR);
            System.out.println("Total page rank : " + (totalPR.getValue()/Math.pow(10, 12)));

        }

        Run.top100("data" + (iteration - 1), otherArgs[1], conf);
        //Run.sampleOutput("data"+(iteration-1), otherArgs[1], conf);
        */
    }

    public static Job performParsingJob(String inputPath, String outputPath,
                                        Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(conf, "Parsing Job");
        job.setJarByClass(Run.class);
        job.setMapperClass(ParserMapper.class);
        job.setReducerClass(ParserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
        return job;

    }

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

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
        return job;

    }

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

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path("data" + interation));

        job.waitForCompletion(true);
        return job;
    }

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


    public static Job top100(String inputPath, String outputPath,
                             Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(conf, "Top 100");
        job.setJarByClass(Run.class);
        job.setMapperClass(TopMapper.class);
        //job.setReducerClass(TopReducer.class);
        job.setReducerClass(SampleReducer.class);
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
}

