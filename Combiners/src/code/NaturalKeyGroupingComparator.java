package code;

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
		super(CompositeKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2){
		CompositeKey ck1 = (CompositeKey)wc1;
		CompositeKey ck2 = (CompositeKey)wc2;
		return ck1.getStationId().compareTo(ck2.getStationId());
	}
	
}
