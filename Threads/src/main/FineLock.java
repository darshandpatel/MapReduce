package main;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This class implements Fine Lock strategy on multiple threads
 * @author Darshan
 *
 */
public class FineLock extends Thread{
	
	private List<String> lines;
	private HashMap<String, HashMap<String, Integer>> records;
	private Boolean includeFibonacci;
	
	FineLock(List<String> lines, HashMap<String, HashMap<String, Integer>> records,
			Boolean includeFibonacci){
		this.lines = lines;
		this.records = records;
		this.includeFibonacci = includeFibonacci;
	}
	
	/**
	 * This methods parse the file lines and adds station Id and its TMAX into 
	 * Records (Accumulated Data Structure) based upon multithreading fine lock
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
			
			if(id.equals("") || type.equals("") || !type.equals(Constant.TMAX)){
				continue;
			}
			else{
				addIntoRecords(id, value);
			}
		}	
	}
		
	int calculateFib(int count){
		
		if(count == 0){
			return 0;
		}else if(count == 1){
			return 1;
		}else{
			return calculateFib(count - 1) + calculateFib(count - 2);
		}
	}

	
	/**
	 * This methods adds the given station Id and its TMAX value in the accumulation data structure
	 * with having fine lock on accumulation data structure
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
				synchronized(values){
					count = values.get("Count");
					sum = values.get("Sum");
					values.put("Count", count+1);
					values.put("Sum", sum+Integer.parseInt(value));
					if(includeFibonacci){
						calculateFib(Constant.fibConst);
					}
				}
			}catch(Exception e){
				//e.printStackTrace();
			}
		}else{
			values = new HashMap<String, Integer>();
			synchronized(values){
				values.put("Count", 1);
				values.put("Sum", Integer.parseInt(value));
				records.put(id, values);
			}
		}
	}
	
	/**
	 * This methods applies the Fine Lock method with multiple threads on given list of lines
	 * @param lines
	 * @param includeFibonnaci : whether to run Fibonnaci(17) code along with normal program run
	 * @return HashMap which key is station ID and value is average TMAX Temperature
	 * @throws InterruptedException
	 */
	public static HashMap<String, Float> runFineLock(List<String> lines,
			Boolean includeFibonnaci) throws InterruptedException{
		
		
		HashMap<String, HashMap<String, Integer>> records = new HashMap<String, HashMap<String, Integer>>();
		Integer nbrOfThreads = 4; // Number of cores in the processors
		Integer totalRecords = lines.size();
		
		List<String> firstPart = lines.subList(0, (int)totalRecords/4);
		List<String> secondPart = lines.subList((int)totalRecords/4, (int)totalRecords/2);
		List<String> thirdPart = lines.subList((int)totalRecords/2, (int)3*totalRecords/4);
		List<String> fourthPart = lines.subList((int)3*totalRecords/4, totalRecords);
		
		FineLock thread1 = new FineLock(firstPart, records, includeFibonnaci);
		FineLock thread2 = new FineLock(secondPart, records, includeFibonnaci);
		FineLock thread3 = new FineLock(thirdPart, records, includeFibonnaci);
		FineLock thread4 = new FineLock(fourthPart, records, includeFibonnaci);
		
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


