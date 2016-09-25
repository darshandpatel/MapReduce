package main;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This class implements No Locking strategies on multiple threads
 * @author Darshan
 *
 */
public class NoLockThreads extends Thread{
		
	private List<String> lines;
	private HashMap<String, HashMap<String, Integer>> records;
	static HashMap<String, Float> avgTMax;
	
	public NoLockThreads(List<String> lines, HashMap<String, HashMap<String, Integer>> records){
		this.lines = lines;
		this.records = records;
	}
	

	/**
	 * This methods parse the file lines and adds station Id and its TMAX into 
	 * Records (Accumulated Data Structure)
	 */
	public void run() {
		 
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
	 
	 /**
		 * This methods applies the No Lock method with multiple threads on given list of lines
		 * @param lines
		 * @return HashMap which key is station ID and value is average TMAX Temperature
		 * @throws InterruptedException
		 */
	 public static HashMap<String, Float> runNoLock(List<String> lines) throws InterruptedException{
			
			
		HashMap<String, HashMap<String, Integer>> records = new HashMap<String, HashMap<String, Integer>>();
		Integer nbrOfThreads = 4; // Number of cores in the processors
		Integer totalRecords = lines.size();
		
		List<String> firstPart = lines.subList(0, (int)totalRecords/4);
		List<String> secondPart = lines.subList((int)totalRecords/4, (int)totalRecords/2);
		List<String> thirdPart = lines.subList((int)totalRecords/2, (int)3*totalRecords/4);
		List<String> fourthPart = lines.subList((int)3*totalRecords/4, totalRecords);
		
		NoLockThreads thread1 = new NoLockThreads(firstPart, records);
		NoLockThreads thread2 = new NoLockThreads(secondPart, records);
		NoLockThreads thread3 = new NoLockThreads(thirdPart, records);
		NoLockThreads thread4 = new NoLockThreads(fourthPart, records);
		
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

