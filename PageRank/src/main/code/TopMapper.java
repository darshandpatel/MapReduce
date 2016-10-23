package code;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.*;


public class TopMapper extends Mapper<Text, Node, DoubleWritable, Text> {

    DoubleWritable pageRank = new DoubleWritable();
    Text pageName = new Text();
    HashMap<String, Double> pagerankMap;

    protected void setup(Context context) throws IOException, InterruptedException {

        pagerankMap = new HashMap<String, Double>();
        System.out.println("Within setup method of Topmapper ************************************************************");
    }

    public void map(Text key, Node value, Context context) throws IOException, InterruptedException {

        pagerankMap.put(key.toString(), value.getPageRank());
        //pageRank.set(value.getPageRank());
        //context.write(pageRank, key);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

        ValueComparator valueComp = new ValueComparator(pagerankMap);
        TreeMap<String, Double> sortedPageRankMap = new TreeMap<String, Double>(valueComp);

        int count = 0;
        Iterator<Map.Entry<String, Double>> iterator = sortedPageRankMap.entrySet().iterator();
        while(iterator.hasNext()){

            if(count < 100){
                Map.Entry<String, Double> pair = iterator.next();
                pageRank.set(pair.getValue());
                pageName.set(pair.getKey());
                context.write(pageRank, pageName);
            }

        }

        context.write(pageRank, pageName);
    }

    class ValueComparator implements Comparator<String> {
        Map<String, Double> map;

        public ValueComparator(Map<String, Double> map) {
            this.map = map;
        }

        public int compare(String name1, String name2) {
            if (map.get(name1) < map.get(name2)) {
                return -1;
            } else {
                return 1;
            } // returning 0 would merge keys
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
