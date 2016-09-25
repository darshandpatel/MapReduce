package main;

public class Fibonacci {
	
	static int n1 = 0;
	static int n2 = 1;
	
	static void calculateFib(int count){
		
		int update = 0;
		count = count - 2;
		while(count > 0){
			update = n1 + n2;
			n1 = n2;
			n2 = update;
			count--;
		}
	}
	
}
