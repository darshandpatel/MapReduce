package code;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

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
