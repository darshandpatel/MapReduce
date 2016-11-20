package main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ColumnMatrixBuildMapper extends Mapper<Text, Node, Cell, Cell>{
	
	HashMap<String, Long> idMap;
	LongWritable column = new LongWritable();
	Text sampleText = new Text("abc");
	CellArray cellArray = new CellArray();
	Cell valueCell = new Cell();
	Cell keyCell = new Cell();
	
	public void setup(Context context) throws IOException{
		
		idMap = new HashMap<String, Long>();
		
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
				idMap.put(parts[0].trim(), Long.parseLong(parts[1]));
			}
			bufferedReader.close();
		}
		System.out.println("Map size :"+idMap.size());
	}
	
	public void map(Text key, Node value, Context context) throws IOException, InterruptedException {
		
		int size = value.getAdjacencyNodes().size();
		//List<Cell> cellList = new ArrayList<Cell>();
		keyCell.setIndex(idMap.get(key.toString()));
		for(Text adjPage : value.getAdjacencyNodes()){
			//Cell cell = new Cell();
			valueCell.setIndex(idMap.get(adjPage.toString()));
			//cell.setColumn(idMap.get(adjPage.toString()));
			valueCell.setContribution(1d/size);
			//cellList.add(cell);
			context.write(keyCell, valueCell);
			//System.out.println("Works perfectly fine : " + row + cell);
		}
	}
}
