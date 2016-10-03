package main;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This class implements CoarseLock functionality on multiple threads
 * @author Darshan
 *
 */
public class CoarseLock extends Thread{
	
	private List<String> lines;
	private HashMap<String, HashMap<String, Integer>> records;
	private Boolean includeFibonacci;
	
	CoarseLock(List<String> lines, HashMap<String, HashMap<String, Integer>> records,
			Boolean includeFibonacci){
		this.lines = lines;
		this.records = records;
		this.includeFibonacci = includeFibonacci;
	}
	
	/**
	 * This methods parse the given lines and adds station Id as key and sum of its TMAX value
	 * and count as value to Records (Accumulated Data Structure) based upon multithreading 
	 * coarse lock scheme
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
			
			if(id.equals("") || type.equals("") || !type.equals(Constant.TMAX)){
				continue;
			}
			else{
				addIntoRecords(id, value);
			}
		}	
	}
	

	/**
	 * This methods adds the given station Id as key and given value in the accumulation 
	 * data structure with having coarse lock on accumulation data structure
	 * @param id : Station ID
	 * @param value : Station TMAX value
	 */
	public void addIntoRecords(String id, String value){
		
		// Course lock on records (Shared data structure between threads)
		synchronized(records){
			
			HashMap<String, Integer> values = null;
			int count=0;
			int sum=0;
			if(records.containsKey(id)){
				try{
					values = records.get(id);
					count = values.get("Count");
					sum = values.get("Sum");
					// Update record value
					values.put("Count", count+1);
					values.put("Sum", sum+Integer.parseInt(value));
					if(includeFibonacci){
						Fibonacci.calculateFib(Constant.FIB_CONST);
					}
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
	
	/**
	 * This methods applies the Coarse Lock method with multiple threads on the given list of lines
	 * @param lines : ArrayList of string
	 * @param includeFibonnaci : whether to run Fibonacci(17) code along with normal program run
	 * @return HashMap where key is station ID and value is average TMAX Temperature
	 * @throws InterruptedException
	 */
	public static HashMap<String, Float> runCoarseLock(List<String> lines,
			Boolean includeFibonnaci) throws InterruptedException{
		
		HashMap<String, HashMap<String, Integer>> records = new HashMap<String, HashMap<String, Integer>>();
		Integer nbrOfThreads = 4; // Number of cores in the processors
		Integer totalRecords = lines.size();
		
		List<String> firstPart = lines.subList(0, (int)totalRecords/4);
		List<String> secondPart = lines.subList((int)totalRecords/4, (int)totalRecords/2);
		List<String> thirdPart = lines.subList((int)totalRecords/2, (int)3*totalRecords/4);
		List<String> fourthPart = lines.subList((int)3*totalRecords/4, totalRecords);
		
		CoarseLock thread1 = new CoarseLock(firstPart, records, includeFibonnaci);
		CoarseLock thread2 = new CoarseLock(secondPart, records, includeFibonnaci);
		CoarseLock thread3 = new CoarseLock(thirdPart, records, includeFibonnaci);
		CoarseLock thread4 = new CoarseLock(fourthPart, records, includeFibonnaci);
		
		thread1.start();
		thread2.start();
		thread3.start();
		thread4.start();
		
		thread1.join();
		thread2.join();
		thread3.join();
		thread4.join();

		HashMap<String, Float> avgTMax = FileLoader.calculateAvgTMax(records);
		return avgTMax;
	}
}


