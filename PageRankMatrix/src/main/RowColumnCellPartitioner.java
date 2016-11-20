package main;

import org.apache.hadoop.mapreduce.Partitioner;

public class RowColumnCellPartitioner extends Partitioner<Cell, Cell> {

	@Override
    public int getPartition(Cell key, Cell value, int nbrOfReducer){
		return (int)(key.getIndex() % nbrOfReducer);
    }
	
}

