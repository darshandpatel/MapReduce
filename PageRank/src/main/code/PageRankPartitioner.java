package code;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Darshan on 10/23/16.
 */
public class PageRankPartitioner extends Partitioner<Text, Node> {

    @Override
    public int getPartition(Text key, Node value, int nbrOfReducer){
        return Math.max(Math.floorMod(key.toString().hashCode(), nbrOfReducer), 0);
    }

}
