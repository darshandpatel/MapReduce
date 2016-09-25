package main;

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
		System.out.println("Please provide any of the below option by entering (1 to 6)");
		System.out.println(String.format("%-10s : %s" , "Option 1", "Sequential Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 2", "Parallel with No Lock Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 3", "Parallel with Coarse Lock Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 4", "Parallel with Fine Lock Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 5", "Parallel with No sharing Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 6", "Quit" ));
	}
	
	/**
	 * This method prints the time difference between startTime and endTime
	 * @param startTime
	 * @param endTime
	 */
	public static void printRunTime(long startTime, long endTime){
		
		System.out.println("Code Run Time (Milliseconds) : "+ (endTime-startTime));
		
	}
	public static void main(String[] args){
		
		String fileLocation;
		
		if(args.length > 0){
			
			fileLocation = args[0];
			List<String> lines = FileLoader.loadFileData(fileLocation);
			
			Boolean flag = true;
			while(flag){
				
				try {
					printOptions();
					int option = readOption();
					long startTime = System.currentTimeMillis();
					long endTime = 0l;
					switch(option){
					case 1:
						// Sequential Run
						SeqThreads.runSeq(lines);
						endTime = System.currentTimeMillis();
						printRunTime(startTime, endTime);
						break;
					case 2:
						// Parallel with No Lock Run
						NoLockThreads.runNoLock(lines);
						 endTime = System.currentTimeMillis();
						 printRunTime(startTime, endTime);
						break;
					case 3:
						// Parallel with coarse Lock
						CoarseLock.runCoarseLock(lines);
						endTime = System.currentTimeMillis();
						printRunTime(startTime, endTime);
						break;
					case 4:
						// Parallel with Fine Lock Run
						FineLock.runFineLock(lines);
						endTime = System.currentTimeMillis();
						printRunTime(startTime, endTime);
						break;
					case 5:
						NoSharing.runNoSharing(lines);
						endTime = System.currentTimeMillis();
						printRunTime(startTime, endTime);
						break;
					case 6:
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
			
		}else{
			System.out.println("Note: This program needs file path as argument.");
			System.out.println("Please provide sufficient arguments. Thank you");
		}
	}

}
