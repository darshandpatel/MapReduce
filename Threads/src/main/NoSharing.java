package main;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This class implements No sharing strategies on multiple threads
 * @author Darshan
 *
 */
public class NoSharing extends Thread{
	
	private List<String> lines;
	private HashMap<String, HashMap<String, Integer>> records;
	private Boolean includeFibonnaci;
	
	NoSharing(List<String> lines, Boolean includeFibonnaci){
		this.lines = lines;
		this.records = new HashMap<String, HashMap<String, Integer>>();
		this.includeFibonnaci = includeFibonnaci;
	}
	
	/**
	 * This methods parse the file lines and adds station Id and its TMAX into 
	 * Records (Accumulated Data Structure) based upon multithreading no sharing
	 * scheme
	 */
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
				addIntoRecords(id, value);
			}
		}	
	
	}
	
	/**
	 * @return Records (Accumulated Data Structure)
	 */
	public HashMap<String, HashMap<String, Integer>> getRecords(){
		return this.records;
	}
	
	/**
	 * This methods adds the given station Id and its TMAX value in the accumulation data structure
	 * @param id : Station ID
	 * @param value : Station TMAX value
	 */
	public void addIntoRecords(String id, String value){
		
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
				if(includeFibonnaci){
					Fibonacci.calculateFib(17);
				}
			}catch(Exception e){
				System.out.println("Check the values");
			}
		}else{
			values = new HashMap<String, Integer>();
			values.put("Count", 1);
			values.put("Sum", Integer.parseInt(value));
			records.put(id, values);
			if(includeFibonnaci){
				Fibonacci.calculateFib(17);
			}
		}
	}
	
	/**
	 * This method merge two given HashMap. If common key exists in the both HashMap then their values are summed up 
	 * in the return HashMap.
	 * @param firstMap
	 * @param secondMap
	 * @return
	 */
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

	
	/**
	 * This methods applies the No sharing strategy with multiple threads on given list of lines
	 * @param lines
	 * @param includeFibonnaci : whether to run Fibonnaci(17) code along with normal program run
	 * @return HashMap which key is station ID and value is average TMAX Temperature
	 * @throws InterruptedException
	 */
	public static HashMap<String, Float> runNoSharing(List<String> lines, 
			Boolean includeFibonnaci) throws InterruptedException{
		
		
		Integer nbrOfThreads = 4; // Number of cores in the processors
		Integer totalRecords = lines.size();
		
		List<String> firstPart = lines.subList(0, (int)totalRecords/4);
		List<String> secondPart = lines.subList((int)totalRecords/4, (int)totalRecords/2);
		List<String> thirdPart = lines.subList((int)totalRecords/2, (int)3*totalRecords/4);
		List<String> fourthPart = lines.subList((int)3*totalRecords/4, totalRecords);
		
		NoSharing thread1 = new NoSharing(firstPart, includeFibonnaci);
		NoSharing thread2 = new NoSharing(secondPart, includeFibonnaci);
		NoSharing thread3 = new NoSharing(thirdPart, includeFibonnaci);
		NoSharing thread4 = new NoSharing(fourthPart, includeFibonnaci);
		
		thread1.start();
		thread2.start();
		thread3.start();
		thread4.start();
		
		thread1.join();
		thread2.join();
		thread3.join();
		thread4.join();
		
		// Collect Records from each threads
		HashMap<String, HashMap<String, Integer>> firstRecords = thread1.getRecords();
		HashMap<String, HashMap<String, Integer>> secondRecords = thread2.getRecords();
		HashMap<String, HashMap<String, Integer>> thirdRecords = thread3.getRecords();
		HashMap<String, HashMap<String, Integer>> forthRecords = thread4.getRecords();
		
		// Merge the calculated Records
		HashMap<String, HashMap<String, Integer>> firstMerge = NoSharing.mergeMap(firstRecords, secondRecords);
		HashMap<String, HashMap<String, Integer>> secondMerge = NoSharing.mergeMap(firstMerge, thirdRecords);
		HashMap<String, HashMap<String, Integer>> thirdMerge = NoSharing.mergeMap(secondMerge, forthRecords);
		
		HashMap<String, Float> avgTMax = FileLoader.calculateAvgTMax(thirdMerge);
		return avgTMax;
	}
}


