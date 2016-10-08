package code;

import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKey, TempStatus>{

	@Override
	public int getPartition(CompositeKey ck, TempStatus ts, int nr) {
		// TODO Auto-generated method stub
		int value = ck.getStationId().hashCode();
		return value % nr;
	}

}
