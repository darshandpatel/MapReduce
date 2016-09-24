package main;


import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CoarseLock extends Thread{
	
	private List<String> lines;
	private HashMap<String, HashMap<String, Integer>> records;
	
	CoarseLock(List<String> lines, HashMap<String, HashMap<String, Integer>> records){
		this.lines = lines;
		this.records = records;
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
	
	public static void addIntoRecords(HashMap<String, HashMap<String, Integer>> records, String id, String value){
		
		synchronized(records){
			
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
	}
	
	public static HashMap<String, Float> runCoarseLock(List<String> lines) throws InterruptedException{
		
		HashMap<String, HashMap<String, Integer>> records = new HashMap<String, HashMap<String, Integer>>();
		Integer nbrOfThreads = 4; // Number of cores in the processors
		Integer totalRecords = lines.size();
		
		List<String> firstPart = lines.subList(0, (int)totalRecords/4);
		List<String> secondPart = lines.subList((int)totalRecords/4, (int)totalRecords/2);
		List<String> thirdPart = lines.subList((int)totalRecords/2, (int)3*totalRecords/4);
		List<String> fourthPart = lines.subList((int)3*totalRecords/4, totalRecords);
		
		CoarseLock thread1 = new CoarseLock(firstPart, records);
		CoarseLock thread2 = new CoarseLock(secondPart, records);
		CoarseLock thread3 = new CoarseLock(thirdPart, records);
		CoarseLock thread4 = new CoarseLock(fourthPart, records);
		
		thread1.start();
		thread2.start();
		thread3.start();
		thread4.start();
		
		thread1.join();
		thread2.join();
		thread3.join();
		thread4.join();

		HashMap<String, Float> avgTMax = FileLoader.calculateTMax(records);
		return avgTMax;
	}
}


