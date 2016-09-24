package main;

import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

public class Run {
	
	public static int readOption(){
		
		try{
			Scanner scanner = new Scanner(System.in);
			int option = scanner.nextInt();
			return option;
		}catch(Exception ex){
			return 0;
		}

	}
	
	public static void printOptions(){
		System.out.println("Please provide any of the below option by entering (1 to 6)");
		System.out.println(String.format("%-10s : %s" , "Option 1", "Sequential Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 2", "Parallel with No Lock Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 3", "Parallel with Coarse Lock Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 4", "Parallel with Fine Lock Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 5", "Parallel with No sharing Run" ));
		System.out.println(String.format("%-10s : %s" , "Option 6", "Quit" ));
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
					switch(option){
					case 1:
						// Sequential Run
						SeqThreads.runSeq(lines);
						break;
					case 2:
						// Parallel with No Lock Run
						NoLockThreads.runNoLock(lines);
						break;
					case 3:
						// Parallel with coarse Lock
						CoarseLock.runCoarseLock(lines);
						break;
					case 4:
						// Parallel with Fine Lock Run
						FineLock.runFineLock(lines);
						break;
					case 5:
						NoSharing.runNoSharing(lines);
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
