package main;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Cell implements WritableComparable<Cell>{
	
	Long index;
	Double contribution;
	Double pageRank;
	Long dummyIndex;
	
	public Cell(){
		index = 0l;
		contribution = 0d;
		pageRank = 0d;
		dummyIndex = 0l;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(index);
		out.writeDouble(contribution);
		out.writeDouble(pageRank);
		out.writeLong(dummyIndex);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		index = in.readLong();
		contribution = in.readDouble();
		pageRank = in.readDouble();
		dummyIndex = in.readLong();
		
	}
	@Override
	public int compareTo(Cell o) {
		// TODO Auto-generated method stub
		int indexCompare = index.compareTo(o.getIndex());
		if(indexCompare == 0){
			return dummyIndex.compareTo(o.getDummyIndex());
		}
		return indexCompare;
	}
	
	public Double getContribution() {
		return contribution;
	}
	public void setContribution(Double contribution) {
		this.contribution = contribution;
	}

	public Double getPageRank() {
		return pageRank;
	}

	public void setPageRank(Double pageRank) {
		this.pageRank = pageRank;
	}


	public Long getIndex() {
		return index;
	}

	public void setIndex(Long index) {
		this.index = index;
	}
	
	public String toString(){
		return "Index : " + Long.toString(index);
	}

	public Long getDummyIndex() {
		return dummyIndex;
	}

	public void setDummyIndex(Long dummyIndex) {
		this.dummyIndex = dummyIndex;
	}

}
