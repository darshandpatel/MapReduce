package main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NoShare implements Runnable{

	List<String> lines;
	public static List<HashMap<String, HashMap<String, Integer>>> recordList;
	HashMap<String, HashMap<String, Integer>> records;
	List<String> subLines;
	Boolean includeFibonacci;
	
	NoShare(List<String> subLines, Boolean includeFibonacci){
		this.subLines = subLines;
		this.records = new HashMap<String, HashMap<String, Integer>>();
		this.includeFibonacci = includeFibonacci;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		String id;
		String type;
		String value;
		
		for(String line : subLines){
			
			String parts[] =line.split(",");
			id = parts[0].trim();
			type = parts[2].trim();
			value = parts[3].trim();
			
			if(id.equals("") || type.equals("") || !type.equals(Constant.TMAX)){
				continue;
			}
			else{
				HashMap<String, Integer> values = null;
				int count=0;
				int sum=0;
				if(records.containsKey(id)){
					values = records.get(id);
					count = values.get("Count");
					sum = values.get("Sum");
					values.put("Count", count+1);
					values.put("Sum", sum+Integer.parseInt(value));
					if(includeFibonacci){
						calculateFib(17);
					}
				}else{
					values = new HashMap<String, Integer>();
					values.put("Count", 1);
					values.put("Sum", Integer.parseInt(value));
					records.put(id, values);
				}
			}
		}	
		recordList.add(records);
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
	
	static HashMap<String, Float> calculateAvgTMax(){
		
		HashMap<String, HashMap<String, Integer>> finalRecord = new HashMap<String, HashMap<String, Integer>>();
		
		for(HashMap<String, HashMap<String, Integer>> records : recordList){
			
			Iterator<Entry<String, HashMap<String, Integer>>> it = records.entrySet().iterator();
			while(it.hasNext()){
				Map.Entry<String, HashMap<String, Integer>> pair = (Map.Entry<String, HashMap<String, Integer>>)it.next();
				if(finalRecord.containsKey(pair.getKey())){
					HashMap<String, Integer> values =  finalRecord.get(pair.getKey());
					values.put("Sum", values.get("Sum") + pair.getValue().get("Sum"));
					values.put("Count", values.get("Count") + pair.getValue().get("Count"));
				}else{
					finalRecord.put(pair.getKey(), pair.getValue());
				}
				
			}
		}
		
		HashMap<String, Float> avgTMax = new HashMap<String, Float>();
		Iterator<Entry<String, HashMap<String, Integer>>> it = finalRecord.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String, HashMap<String, Integer>> pair = (Map.Entry<String, HashMap<String, Integer>>)it.next();
			Float avg = (float)pair.getValue().get("Sum") / (float)pair.getValue().get("Count");
			avgTMax.put(pair.getKey(), avg);
		}
		return avgTMax;
		
	}

	public static void start(List<String> lines, Boolean includeFibonacci){
		
		try {
			
			Integer nbrOfThreads = 4; // Number of cores in the processors
			Integer totalRecords = lines.size();
			
			recordList = new ArrayList<HashMap<String, HashMap<String, Integer>>>();
			
			List<String> firstPart = lines.subList(0, (int)totalRecords/4);
			List<String> secondPart = lines.subList((int)totalRecords/4, (int)totalRecords/2);
			List<String> thirdPart = lines.subList((int)totalRecords/2, (int)3*totalRecords/4);
			List<String> fourthPart = lines.subList((int)3*totalRecords/4, totalRecords);
			
			ExecutorService executorService = Executors.newFixedThreadPool(nbrOfThreads);
			
			executorService.execute(new NoShare(firstPart, includeFibonacci));
			executorService.execute(new NoShare(secondPart, includeFibonacci));
			executorService.execute(new NoShare(thirdPart, includeFibonacci));
			executorService.execute(new NoShare(fourthPart, includeFibonacci));
			
			executorService.shutdown();
			executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			calculateAvgTMax();
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
