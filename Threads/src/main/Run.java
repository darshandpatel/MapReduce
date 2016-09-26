package main;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

/**
 * This class is main class of the application
 * @author Darshan
 *
 */
public class Run {
	
	/**
	 * This methods read the command line input
	 * @return
	 */
	public static int readOption(){
		
		try{
			Scanner scanner = new Scanner(System.in);
			int option = scanner.nextInt();
			return option;
		}catch(Exception ex){
			return 0;
		}

	}
	
	/**
	 * This method prints the available options
	 */
	public static void printOptions(){
		System.out.println("Please provide any of the below option by entering (1 to 11)");
		System.out.println(String.format("%-10s : %s" , "Option 1", "Sequential Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 2", "Sequential with Fibonacci(17) Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 3", "Parallel with No Lock Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 4", "Parallel with No Lock and Fibonacci(17) Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 5", "Parallel with Coarse Lock Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 6", "Parallel with Coarse Lock and Fibonacci(17) Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 7", "Parallel with Fine Lock Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 8", "Parallel with Fine Lock and Fibonacci(17) Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 9", "Parallel with No sharing Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 10", "Parallel with No sharing and Fibonacci(17) Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 11", "Quit" ));
	}
	
	public static void printTestResult(HashMap<String, Float> avgTMax){
		String key = "USC00242347";
		System.out.println(key + " average temperature : "+ avgTMax.get(key));
	}
	
	/**
	 * This method prints the time difference between startTime and endTime
	 * @param startTime
	 * @param endTime
	 */
	public static void printRunTime(long startTime, long endTime){
		
		System.out.println("Code Run Time (Milliseconds) : "+ (endTime-startTime));
		
	}
	
	public static void multipleRun(List<String> lines, int option, int nbrOfRun){
		
		List<Float> runTimes = new ArrayList<Float>(nbrOfRun);
		
		HashMap<String, Float> avgTMax;
		for(int i=0; i<nbrOfRun; i++){
			try {
				long startTime = System.currentTimeMillis();
				long endTime = 0l;
				switch(option){
				case 1:
					// Sequential Run
					System.out.println("Sequential Run");
					avgTMax = SeqThreads.runSeq(lines, false);
					endTime = System.currentTimeMillis();
					runTimes.add((float) (endTime-startTime));
					break;
				case 2:
					// Sequential Run with Fibonacci(17)
					System.out.println("Sequential Run with Fibonacci(17)");
					avgTMax = SeqThreads.runSeq(lines, true);
					endTime = System.currentTimeMillis();
					runTimes.add((float) (endTime-startTime));
					break;
				case 3:
					// Parallel with No Lock Run
					System.out.println("Parallel with No Lock Run");
					avgTMax = NoLockThreads.runNoLock(lines, false);
					endTime = System.currentTimeMillis();
					runTimes.add((float) (endTime-startTime));
					break;
				case 4:
					// Parallel with No Lock and Fibonacci(17) Run
					System.out.println("Parallel with No Lock and Fibonacci(17) Run");
					avgTMax = NoLockThreads.runNoLock(lines, true);
					endTime = System.currentTimeMillis();
					runTimes.add((float) (endTime-startTime));
					break;
				case 5:
					// Parallel with coarse Lock
					System.out.println("Parallel with coarse Lock");
					avgTMax = CoarseLock.runCoarseLock(lines, false);
					endTime = System.currentTimeMillis();
					runTimes.add((float) (endTime-startTime));
					break;
				case 6:
					// Parallel with coarse Lock and Fibonacci(17) Run
					System.out.println("Parallel with coarse Lock and Fibonacci(17) Run");
					avgTMax = CoarseLock.runCoarseLock(lines, true);
					endTime = System.currentTimeMillis();
					runTimes.add((float) (endTime-startTime));
					break;
				case 7:
					// Parallel with Fine Lock Run
					System.out.println("Parallel with Fine Lock Run");
					avgTMax = FineLock.runFineLock(lines, false);
					endTime = System.currentTimeMillis();
					runTimes.add((float) (endTime-startTime));
					break;
				case 8:
					// Parallel with Fine Lock and Fibonacci(17) Run
					System.out.println("Parallel with Fine Lock and Fibonacci(17) Run");
					avgTMax = FineLock.runFineLock(lines, true);
					endTime = System.currentTimeMillis();
					runTimes.add((float) (endTime-startTime));
					break;
				case 9:
					// Parallel with No Sharing Run
					System.out.println("Parallel with No Sharing Run");
					avgTMax = NoSharing.runNoSharing(lines, false);
					endTime = System.currentTimeMillis();
					runTimes.add((float) (endTime-startTime));
					break;
				case 10:
					// Parallel with No Sharing and Fibonacci(17) Run
					System.out.println("Parallel with No Sharing and Fibonacci(17) Run");
					avgTMax = NoSharing.runNoSharing(lines, true);
					endTime = System.currentTimeMillis();
					runTimes.add((float) (endTime-startTime));
					break;
				default:
					System.out.println("Please provide the correct option. Thank you");
				}
				
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		float sum = 0.0f;
		for (float rtime: runTimes) sum += rtime;
		float avgTime = sum/runTimes.size();
		System.out.println("Minimum Run Times : " + Collections.min(runTimes));
		System.out.println("Maximum Run Times : " + Collections.max(runTimes));
		System.out.println("Average Run Times : " + avgTime);
	}
	
	/**
	 * This method takes user input and run the threads based upon it.
	 * @param lines
	 */
	public static void optionRun(List<String> lines){
		
		Boolean flag = true;
		HashMap<String, Float> avgTMax;
		
		while(flag){
			
			try {
				printOptions();
				int option = readOption();
				long startTime = System.currentTimeMillis();
				long endTime = 0l;
				switch(option){
				case 1:
					// Sequential Run
					avgTMax = SeqThreads.runSeq(lines, false);
					endTime = System.currentTimeMillis();
					printRunTime(startTime, endTime);
					printTestResult(avgTMax);
					break;
				case 2:
					// Sequential Run with Fibonacci(17)
					avgTMax = SeqThreads.runSeq(lines, true);
					endTime = System.currentTimeMillis();
					printRunTime(startTime, endTime);
					printTestResult(avgTMax);
					break;
				case 3:
					// Parallel with No Lock Run
					avgTMax = NoLockThreads.runNoLock(lines, false);
					endTime = System.currentTimeMillis();
					printRunTime(startTime, endTime);
					printTestResult(avgTMax);
					break;
				case 4:
					// Parallel with No Lock and Fibonacci(17) Run
					avgTMax = NoLockThreads.runNoLock(lines, true);
					endTime = System.currentTimeMillis();
					printRunTime(startTime, endTime);
					printTestResult(avgTMax);
					break;
				case 5:
					// Parallel with coarse Lock
					avgTMax = CoarseLock.runCoarseLock(lines, false);
					endTime = System.currentTimeMillis();
					printRunTime(startTime, endTime);
					printTestResult(avgTMax);
					break;
				case 6:
					// Parallel with coarse Lock and Fibonacci(17) Run
					avgTMax = CoarseLock.runCoarseLock(lines, true);
					endTime = System.currentTimeMillis();
					printRunTime(startTime, endTime);
					printTestResult(avgTMax);
					break;
				case 7:
					// Parallel with Fine Lock Run
					avgTMax = FineLock.runFineLock(lines, false);
					endTime = System.currentTimeMillis();
					printTestResult(avgTMax);
					printRunTime(startTime, endTime);
					break;
				case 8:
					// Parallel with Fine Lock and Fibonacci(17) Run
					avgTMax = FineLock.runFineLock(lines, true);
					endTime = System.currentTimeMillis();
					printTestResult(avgTMax);
					printRunTime(startTime, endTime);
					break;
				case 9:
					// Parallel with No Sharing Run
					avgTMax = NoSharing.runNoSharing(lines, false);
					endTime = System.currentTimeMillis();
					printRunTime(startTime, endTime);
					printTestResult(avgTMax);
					break;
				case 10:
					// Parallel with No Sharing and Fibonacci(17) Run
					avgTMax = NoSharing.runNoSharing(lines, true);
					endTime = System.currentTimeMillis();
					printRunTime(startTime, endTime);
					printTestResult(avgTMax);
					break;
				case 11:
					flag=false;
					break;
				default:
					System.out.println("Please provide the correct option. Thank you");
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args){
		
		String fileLocation;
		
		if(args.length > 0){
			
			fileLocation = args[0];
			List<String> lines = FileLoader.loadFileData(fileLocation);
			
			/*
			for(int j=1; j<11;j++){
				Run.multipleRun(lines, j, 10);
			}
			*/
			Run.optionRun(lines);
			
			
		}else{
			System.out.println("Note: This program needs file path as argument.");
			System.out.println("Please provide sufficient arguments. Thank you");
		}
	}

}
