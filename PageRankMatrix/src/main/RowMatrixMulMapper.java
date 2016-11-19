package main;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class RowMatrixMulMapper extends Mapper<LongWritable, Cell, LongWritable, Cell> {

	public void map(LongWritable key, Cell value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}
}
