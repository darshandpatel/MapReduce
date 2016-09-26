package main;

import java.util.*;


/**
 * This class implements sequential code to calculate average maximum temperature of each station.
 * @author Darshan
 *
 */
public class SeqThreads {
	
	/**
	 * This method analyze each line and puts station Id and its temperature in HashMap
	 * @param lines : List of file lines
	 * @param records : DataStructure containing station Id and its corresponding Max temperature 
	 * total and count
	 * @param includeFibonnaci : whether to run Fibonnaci(17) code along with normal program run
	 */
	public static void collectRecords(List<String> lines,
			HashMap<String, HashMap<String, Integer>> records,
			Boolean includeFibonacci){
		
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
			
			if(id.equals("") || type.equals("") || !type.equals(Constant.TMAX)){
				continue;
			}
			else{
				if(records.containsKey(id)){
					values = records.get(id);
					count = values.get("Count");
					sum = values.get("Sum");
					values.put("Count", count+1);
					values.put("Sum", sum+Integer.parseInt(value));
					if(includeFibonacci){
						Fibonacci.calculateFib(Constant.fibConst);
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
	 * This methods run the sequential code to calculate average TMAX per station on the given lines of inputs.
	 * @param lines
	 * @param includeFibonnaci : whether to run Fibonnaci(17) code with normal run
	 * @return HashMap which contain station id as key and its average TMAX as values
	 * @throws InterruptedException
	 */
	public static HashMap<String, Float> runSeq(List<String> lines, Boolean includeFibonnaci) throws InterruptedException{
		
		String fileLocation;
		
		HashMap<String, HashMap<String, Integer>>  records = new HashMap<String, HashMap<String, Integer>>();
		SeqThreads.collectRecords(lines, records, includeFibonnaci);
		
		HashMap<String, Float> avgTMax = FileLoader.calculateAvgTMax(records);
		return avgTMax;
	}
}


