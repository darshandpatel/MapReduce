package main;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

public class CellArray implements WritableComparable<CellArray> {

	private List<Cell> cells;
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(cells.size());
        for (Cell cell : cells) {
        	cell.write(out);
        }
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int length = in.readInt();
		cells = new LinkedList<Cell>();
        for (int i = 0; i < length; i++) {
        	Cell cell = new Cell();
            cell.readFields(in);
            cells.add(cell);
        }
	}

	@Override
	public int compareTo(CellArray o) {
		// TODO Auto-generated method stub
		return 0;
	}

	public List<Cell> getCells() {
		return cells;
	}

	public void setCells(List<Cell> cells) {
		this.cells = cells;
	}
	
}
