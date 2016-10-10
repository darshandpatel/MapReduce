package code;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * This class assign the reducer to the records based upon Hashcode logic
 * @author Darshan
 *
 */
public class NaturalKeyPartitioner extends Partitioner<CompositeKey, TempStatus>{

	@Override
	public int getPartition(CompositeKey ck, TempStatus ts, int nr) {
		// TODO Auto-generated method stub
		int value = ck.getStationId().hashCode() & Integer.MAX_VALUE;
		return value % nr;
	}

}
