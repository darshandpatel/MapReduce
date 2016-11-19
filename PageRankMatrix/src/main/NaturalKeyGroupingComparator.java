package main;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * This class is used to group the records by the specific natural
 * key in the given composite key
 *  Here compare method only compares the natural key (Station id) for 
 *  the composite key class so same station id data will be processed
 *  in one reducer class
 * @author Darshan
 *
 */
public class NaturalKeyGroupingComparator extends WritableComparator{
	
	public NaturalKeyGroupingComparator(){
		super(Cell.class, true);
	}
	
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2){
		
		Cell ck1 = (Cell)wc1;
		Cell ck2 = (Cell)wc2;
		
		return ck1.getIndex().compareTo(ck2.getIndex());
		
	}
	
}
