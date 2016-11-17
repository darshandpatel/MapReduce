package main;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Darshan on 10/23/16.
 */
public class PageRankPartitioner extends Partitioner<Text, Node> {

    @Override
    public int getPartition(Text key, Node value, int nbrOfReducer){
        int reducerNumber = Math.floorMod(key.toString().hashCode(), nbrOfReducer);
        return Math.max(reducerNumber, 0);
    }

}
