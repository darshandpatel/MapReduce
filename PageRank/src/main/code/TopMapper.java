package code;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.*;


public class TopMapper extends Mapper<Text, Node, DoubleWritable, Text> {

    DoubleWritable pageRank = new DoubleWritable();
    Text pageName = new Text();
    HashMap<String, Double> pagerankMap;
    double alpha;
    long pageCount;
    double danglingNodesPRSum;
    double constantPRAdd;
    double newPageRank;

    protected void setup(Context context) throws IOException, InterruptedException {

        pagerankMap = new HashMap<String, Double>();
        /*
        Configuration conf = context.getConfiguration();
        pageCount = conf.getLong(Constant.PAGE_COUNT, -10L);
        alpha = conf.getDouble(Constant.ALPHA, -10);
        long tempDanglingNodesPRSum = conf.getLong(Constant.DANGLING_NODES_PR_SUM, 0);
        danglingNodesPRSum = tempDanglingNodesPRSum / Math.pow(10, Constant.POWER);
        constantPRAdd = (1-alpha) * (danglingNodesPRSum/pageCount);
        */

    }

    public void map(Text key, Node value, Context context) throws IOException, InterruptedException {

        //newPageRank = value.getPageRank() + constantPRAdd;
        pagerankMap.put(key.toString(), value.getPageRank());
        //context.getCounter(COUNTERS.TOTAL_PR).increment((long) (newPageRank * Math.pow(10, Constant.POWER)));
        //pageRank.set(value.getPageRank());
        //context.write(pageRank, key);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

        int count = 0;

        List<Map.Entry<String, Double>> list = new LinkedList(pagerankMap.entrySet());
        // Defined Custom Comparator here
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                return ((Comparable) ((Map.Entry)(o2)).getValue())
                        .compareTo(((Map.Entry)(o1)).getValue());
            }
        });

        for(Iterator it = list.iterator(); it.hasNext();) {
            if(count < 100){
                Map.Entry<String, Double> pair = (Map.Entry<String, Double>) it.next();
                pageRank.set(pair.getValue());
                pageName.set(pair.getKey());
                context.write(pageRank, pageName);
                count++;
            }else {
                break;
            }
        }
    }

}


class SampleMapper extends Mapper<Text, Node, Text, DoubleWritable> {

    DoubleWritable pageRank = new DoubleWritable();

    public void map(Text key, Node value, Context context) throws IOException, InterruptedException {

        pageRank.set(value.getPageRank());
        context.write(key, pageRank);
    }

}
