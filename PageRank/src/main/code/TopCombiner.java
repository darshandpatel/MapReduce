package code;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TopCombiner extends Reducer<LongWritable, Text, LongWritable, Text> {

    int counter;

    public void setup() {
        counter = 0;
    }

    public void reduce(LongWritable key, Iterable<Text> pages,
                       Context context) throws IOException, InterruptedException {

        if (counter < 100) {
            for (Text page : pages) {
                context.write(key, page);
                counter += 1;
                if (counter > 99) {
                    break;
                }
            }
        }

    }
}
