package main;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class NoSharing extends Thread{
	
	private List<String> lines;
	private HashMap<String, HashMap<String, Integer>> records;
	
	NoSharing(List<String> lines){
		this.lines = lines;
		this.records = new HashMap<String, HashMap<String, Integer>>();
	}
	
	public void run() {
		
		String id;
		String type;
		String value;
		
		for(String line : lines){
			
			String parts[] =line.split(",");
			id = parts[0].trim();
			type = parts[2].trim();
			value = parts[3].trim();
			
			if(id.equals("") && type.equals("") && !type.equals(Constant.TMAX)){
				continue;
			}
			else{
				CoarseLock.addIntoRecords(this.records, id, value);
			}
		}	
	
	}
	
	public HashMap<String, HashMap<String, Integer>> getRecords(){
		return this.records;
	}
	
	public static void addIntoRecords(HashMap<String, HashMap<String, Integer>> records, String id, String value){
		
		HashMap<String, Integer> values = null;
		int count=0;
		int sum=0;
		if(records.containsKey(id)){
			try{
				values = records.get(id);
			
				count = values.get("Count");
				sum = values.get("Sum");
				values.put("Count", count+1);
				values.put("Sum", sum+Integer.parseInt(value));
				
			}catch(Exception e){
				System.out.println("Check the values");
			}
		}else{
			values = new HashMap<String, Integer>();
			values.put("Count", 1);
			values.put("Sum", Integer.parseInt(value));
			records.put(id, values);
			
		}
		
	}
	
	public static HashMap<String, HashMap<String, Integer>> mergeMap(HashMap<String, HashMap<String, Integer>> firstMap,
			HashMap<String, HashMap<String, Integer>> secondMap){
		
		HashMap<String, HashMap<String, Integer>> returnMap = new HashMap<String, HashMap<String, Integer>>();
		returnMap.putAll(firstMap);
		
		for(String key : secondMap.keySet()) {
		    if(returnMap.containsKey(key)) {
		    	HashMap<String, Integer> values = returnMap.get(key);
		    	values.put("Sum", values.get("Sum") + secondMap.get(key).get("Sum"));
		    	values.put("Count", values.get("Count") + secondMap.get(key).get("Count"));
		    } else {
		    	returnMap.put(key,secondMap.get(key));
		    }
		}

		return returnMap;
	}

	public static HashMap<String, Float> runNoSharing(List<String> lines) throws InterruptedException{
		
		
		Integer nbrOfThreads = 4; // Number of cores in the processors
		Integer totalRecords = lines.size();
		
		List<String> firstPart = lines.subList(0, (int)totalRecords/4);
		List<String> secondPart = lines.subList((int)totalRecords/4, (int)totalRecords/2);
		List<String> thirdPart = lines.subList((int)totalRecords/2, (int)3*totalRecords/4);
		List<String> fourthPart = lines.subList((int)3*totalRecords/4, totalRecords);
		
		NoSharing thread1 = new NoSharing(firstPart);
		NoSharing thread2 = new NoSharing(secondPart);
		NoSharing thread3 = new NoSharing(thirdPart);
		NoSharing thread4 = new NoSharing(fourthPart);
		
		thread1.start();
		thread2.start();
		thread3.start();
		thread4.start();
		
		thread1.join();
		thread2.join();
		thread3.join();
		thread4.join();
		
		HashMap<String, HashMap<String, Integer>> firstRecords = thread1.getRecords();
		HashMap<String, HashMap<String, Integer>> secondRecords = thread2.getRecords();
		HashMap<String, HashMap<String, Integer>> thirdRecords = thread3.getRecords();
		HashMap<String, HashMap<String, Integer>> forthRecords = thread4.getRecords();
		
		HashMap<String, HashMap<String, Integer>> firstMerge = NoSharing.mergeMap(firstRecords, secondRecords);
		HashMap<String, HashMap<String, Integer>> secondMerge = NoSharing.mergeMap(firstMerge, thirdRecords);
		HashMap<String, HashMap<String, Integer>> thirdMerge = NoSharing.mergeMap(secondMerge, forthRecords);
		
		HashMap<String, Float> avgTMax = FileLoader.calculateTMax(thirdMerge);
		return avgTMax;
	}
}


