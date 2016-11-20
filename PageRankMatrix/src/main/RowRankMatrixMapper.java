package main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RowRankMatrixMapper extends Mapper<Object, Text, Cell, Cell>{
	
	int iteration;
	LongWritable index = new LongWritable();
	Cell keyCell = new Cell();
	Cell valueCell = new Cell();
	
	public void setup(Context context){
		Configuration conf =  context.getConfiguration();
		iteration = conf.getInt(Constant.ITERATION, -10);
	}
	
	public void map(Object object, Text text, Context context) throws IOException, InterruptedException {
		
		if(iteration == 1){
			String parts[] = text.toString().split("\t");
			keyCell.setIndex(Long.parseLong(parts[1]));
			valueCell.setPageRank(Double.parseDouble(parts[2]));
		}else{
			String parts[] = text.toString().split("\t");
			keyCell.setIndex(Long.parseLong(parts[0]));
			valueCell.setPageRank(Double.parseDouble(parts[1]));
		}
		keyCell.setDummyIndex(-1l);
		//System.out.println(keyCell.toString() + " ::: "+Double.toString(valueCell.getPageRank()));
		context.write(keyCell, valueCell);
	}

}
