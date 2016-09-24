package main;

import java.util.*;


public class SeqThreads {
	
	public static void collectRecords(List<String> lines,
			HashMap<String, HashMap<String, Integer>> records){
		
		HashMap<String, Integer> values = null;
		
		String id;
		String type;
		String value;
		
		int count=0;
		int sum=0;
		
		for(String line : lines){
			
			String parts[] =line.split(",");
			id = parts[0].trim();
			type = parts[2].trim();
			value = parts[3].trim();
			
			if(id.equals("") && type.equals("") && !type.equals(Constant.TMAX)){
				continue;
			}
			else{
				if(records.containsKey(id)){
					try{
						values = records.get(id);
						count = values.get("Count");
						sum = values.get("Sum");
						values.put("Count", count+1);
						values.put("Sum", sum+Integer.parseInt(value));
					}catch(Exception e){
						//e.printStackTrace();
					}
				}else{
					values = new HashMap<String, Integer>();
					values.put("Count", 1);
					values.put("Sum", Integer.parseInt(value));
					records.put(id, values);
				}
			}
		}	
	}
	
	public static HashMap<String, Float> runSeq(List<String> lines) throws InterruptedException{
		
		String fileLocation;
		
		HashMap<String, HashMap<String, Integer>>  records = new HashMap<String, HashMap<String, Integer>>();
		SeqThreads.collectRecords(lines, records);
		
		HashMap<String, Float> avgTMax = FileLoader.calculateTMax(records);
		return avgTMax;
	}
}


