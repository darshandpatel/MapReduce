package main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
		Path sourceFilePath = new Path(cacheFiles[0]);
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(sourceFilePath.toUri(), conf);
		FileStatus[] status = fs.listStatus(sourceFilePath);
		for (int i=0;i<status.length;i++){
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
			String line = null;
			while((line = bufferedReader.readLine()) != null){
				String[] parts = line.split("\t");
				//System.out.println(parts[0]+" : "+parts[1]+" : "+parts[2]);
				idMap.put(Long.parseLong(parts[1]), parts[0].trim());
			}
			bufferedReader.close();
		}
		
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