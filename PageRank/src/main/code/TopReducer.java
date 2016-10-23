package code;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * TopReducer only emit the first 100 records its receives.
 */
public class TopReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

    int counter;

    public void setup() {
        counter = 0;
    }

    public void reduce(DoubleWritable key, Iterable<Text> pages,
                       Context context) throws IOException, InterruptedException {

        if (counter < 100) {
            for (Text page : pages) {
                counter += 1;
                if (counter > 99) {
                    break;
                } else {
                    context.write(page, key);
                }
            }
        }

    }
}


class SampleReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for(Text value : values){
            context.write(value, key);
        }

    }

}
