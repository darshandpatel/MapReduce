package main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RowMatrixBuildMapper extends Mapper<Text, Node, LongWritable, Cell>{
	
	HashMap<String, Long> idMap;
	LongWritable row = new LongWritable();
	Text sampleText = new Text("abc");
	CellArray cellArray = new CellArray();
	Cell cell = new Cell();
	
	public void setup(Context context) throws IOException{
		
		idMap = new HashMap<String, Long>();
		
		URI[] cacheFiles = context.getCacheFiles();
		String sourceFilePath = new Path(cacheFiles[0]).toString();
		System.out.println("sourceFilePath : "+sourceFilePath);
		BufferedReader bufferedReader = new BufferedReader(new FileReader(sourceFilePath));
		String line = null;
		while((line = bufferedReader.readLine()) != null){
			String[] parts = line.split("\t");
			//System.out.println(parts[0]+" : "+parts[1]+" : "+parts[2]);
			idMap.put(parts[0].trim(), Long.parseLong(parts[1]));
		}
		System.out.println("Map size :"+idMap.size());
		bufferedReader.close();
	}
	
	public void map(Text key, Node value, Context context) throws IOException, InterruptedException {
		
		int size = value.getAdjacencyNodes().size();
		//List<Cell> cellList = new ArrayList<Cell>();
		cell.setIndex(idMap.get(key.toString()));
		for(Text adjPage : value.getAdjacencyNodes()){
			//Cell cell = new Cell();
			row.set(idMap.get(adjPage.toString()));
			cell.setContribution(1d/size);
			//cellList.add(cell);
			context.write(row, cell);
			//System.out.println("Works perfectly fine : " + row + cell);
		}
		//context.write(row, cell);
		//cellArray.setCells(cellList);
		//context.write(row, cellArray);
	}
}

class PrintRowMatrixBuildMapper extends Mapper<LongWritable, Cell, LongWritable, Text>{
	
	Text returnText = new Text();
	public void map(LongWritable key, Cell value, Context context) throws IOException, InterruptedException {
		System.out.println("**********************************");
		String strReturn = new String(value.getIndex() + " : "+value.getContribution());
		returnText.set(strReturn);
		context.write(key, returnText);
	}
}
