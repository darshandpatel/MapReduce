package main;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Cell implements WritableComparable<Cell>{
	
	Long row;
	Long column;
	Double contribution;
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(row);
		out.writeLong(column);
		out.writeDouble(contribution);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		row = in.readLong();
		column = in.readLong();
		contribution = in.readDouble();
		
	}
	@Override
	public int compareTo(Cell o) {
		// TODO Auto-generated method stub
		int rowCompare = row.compareTo(o.row);
		
		if(rowCompare == 0){
			return column.compareTo(o.column);
		}
		
		return rowCompare;
	}
	
	public Long getRow() {
		return row;
	}
	public void setRow(Long row) {
		this.row = row;
	}
	public Long getColumn() {
		return column;
	}
	public void setColumn(Long column) {
		this.column = column;
	}
	
	public Double getContribution() {
		return contribution;
	}
	public void setContribution(Double contribution) {
		this.contribution = contribution;
	}
	
	
}
