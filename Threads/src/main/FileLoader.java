package main;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class FileLoader {
	
	public static List<String> loadFileData(String fileLocation) {
		
		try{
		
			List<String> lines = new ArrayList<String>();
			
			FileInputStream fileInputStream = new FileInputStream(fileLocation);
			GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
			BufferedReader bufferReader = new BufferedReader(new InputStreamReader(gzipInputStream), 1024);
			
			String line;
			while((line = bufferReader.readLine()) != null){
				lines.add(line);
			}
			bufferReader.close();
			return lines;
		
		}catch(FileNotFoundException e){
			System.out.println("Provide file doesn't exist");
			return null;
		} catch (IOException e) {
			System.out.println("Error while reading file");
			return null;
		}
	}
	
	

	public static HashMap<String, Float> calculateTMax(HashMap<String, HashMap<String, Integer>> records){
		
		HashMap<String, Float> avgTMax = new HashMap<String, Float>();
		Iterator it = records.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String, HashMap<String, Integer>> pair = (Map.Entry<String, HashMap<String, Integer>>)it.next();
			Float avg = (float)pair.getValue().get("Sum") / (float)pair.getValue().get("Count");
			avgTMax.put(pair.getKey(), avg);
		}
		return avgTMax;
	}
	
}
