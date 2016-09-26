package main;

public class Fibonacci {
	
	static int n1 = 0;
	static int n2 = 1;
	
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
