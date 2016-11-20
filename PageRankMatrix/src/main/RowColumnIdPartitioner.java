package main;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class RowColumnIdPartitioner extends Partitioner<LongWritable, DoubleWritable> {

	@Override
    public int getPartition(LongWritable key, DoubleWritable value, int nbrOfReducer){
		return (int)(key.get() % nbrOfReducer);
    }
	
}

