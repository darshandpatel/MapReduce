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
    Text outputText = new Text();
    public void setup(Context context) throws IOException, InterruptedException{
        counter = 1;
        System.out.println("Setup is called");
    }

    public void reduce(DoubleWritable key, Iterable<Text> pages,
                       Context context) throws IOException, InterruptedException {

        if (counter <= 100) {
            for (Text page : pages) {
                if (counter > 100) {
                    break;
                } else {
                    outputText.set(counter + " " +page.toString());
                    context.write(outputText, key);
                    counter += 1;
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
