package main;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class FileLoader {
	
	static List<String> lines;
	
	static void loadFileData(String fileLocation) {
		
		try{
		
			lines = new ArrayList<String>();
			
			FileInputStream fileInputStream = new FileInputStream(fileLocation);
			GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
			BufferedReader bufferReader = new BufferedReader(new InputStreamReader(gzipInputStream), 1024);
			
			String line;
			while((line = bufferReader.readLine()) != null){
				lines.add(line);
			}
			bufferReader.close();
		
		}catch(FileNotFoundException e){
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	static List<String> getLines(){
		return lines;
	}
	
	public static void main(String[] args){
		
		String fileLocation;
		if(args.length > 0){
			fileLocation = args[0];
			System.out.println(fileLocation);
			FileLoader.loadFileData(fileLocation);
			System.out.println(FileLoader.getLines());
			System.out.println(FileLoader.getLines().size());
		}
		
	}

}
