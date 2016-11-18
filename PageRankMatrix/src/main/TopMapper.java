package main;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TopMapper extends Mapper<Object, Text, DoubleWritable, LongWritable> {

    DoubleWritable pageRank = new DoubleWritable();
    LongWritable pageId = new LongWritable();
    PriorityQueue<PageInfo> queue;
    
    double alpha;
    long pageCount;
    double danglingNodesPRSum;
    double constantPRAdd;
    double newPageRank;

    protected void setup(Context context) throws IOException, InterruptedException {

        queue = new PriorityQueue<>(100, new Comparator<PageInfo>() {
            public int compare(PageInfo pageInfo1, PageInfo pageInfo2) {
                return pageInfo1.getPageRank().compareTo(pageInfo2.getPageRank());
            }
        });

    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    	String parts[] = value.toString().split("\t");
    	
        PageInfo pageInfo = new PageInfo(Long.parseLong(parts[0]), 
        		Double.parseDouble(parts[1]));
        queue.add(pageInfo);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

        Iterator<PageInfo> itr = queue.iterator();
        while(itr.hasNext()){
        	PageInfo pageInfo = queue.poll();
            // do some processing with str
        	pageId.set(pageInfo.getPageId());
        	pageRank.set(pageInfo.getPageRank());
        	context.write(pageRank, pageId);
        }
    }
}

class SampleMapper extends Mapper<LongWritable, DoubleWritable, DoubleWritable, LongWritable> {
	public void map(LongWritable key, DoubleWritable value, Context context) throws IOException, InterruptedException {
		context.write(value, key);
	}
	
}
