package main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * TopReducer only emit the first 100 records its receives.
 */
public class TopReducer extends Reducer<DoubleWritable, LongWritable, Text, DoubleWritable> {

    int counter;
    Text pageName = new Text();
    HashMap<Long, String> idMap;
    
    public void setup(Context context) throws IOException, InterruptedException{
        counter = 1;
        
        idMap = new HashMap<Long, String>();
		
		URI[] cacheFiles = context.getCacheFiles();
		String sourceFilePath = new Path(cacheFiles[0]).toString();
		System.out.println("sourceFilePath : "+sourceFilePath);
		BufferedReader bufferedReader = new BufferedReader(new FileReader(sourceFilePath));
		String line = null;
		while((line = bufferedReader.readLine()) != null){
			String[] parts = line.split("\t");
			//System.out.println(parts[0]+" : "+parts[1]+" : "+parts[2]);
			idMap.put(Long.parseLong(parts[1]), parts[0].trim());
		}
		System.out.println("**** Map size :"+idMap.size());
		bufferedReader.close();
    }

    public void reduce(DoubleWritable key, Iterable<LongWritable> pages,
                       Context context) throws IOException, InterruptedException {

        if (counter <= 100) {
            for (LongWritable page : pages) {
                if (counter > 100) {
                    break;
                } else {
                	pageName.set(idMap.get(page.get()));
                    context.write(pageName, key);
                    counter += 1;
                }
            }
        }

    }
}

class SampleReducer extends Reducer<DoubleWritable, LongWritable, LongWritable, DoubleWritable> {

    public void reduce(DoubleWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    	for(LongWritable value : values){
    		context.write(value, key);
    	}
            
    }

}