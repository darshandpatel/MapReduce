package main;

public class Fibonacci {
	
	static int n1 = 0;
	static int n2 = 1;
	
	/**
	 * This method calculate the Fibonacci series number based upon
	 * the given counter value.
	 */
	static int calculateFib(int count){
		
		if(count == 0){
			return 0;
		}else if(count == 1){
			return 1;
		}else{
			return calculateFib(count - 1) + calculateFib(count - 2);
		}
	}
	
}
