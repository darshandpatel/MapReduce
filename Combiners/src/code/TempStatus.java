package code;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TempStatus implements Writable{

	int year;
	int tmax;
	int tmin;
	boolean isTmax;
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getTmax() {
		return tmax;
	}

	public void setTmax(int tmax) {
		this.tmax = tmax;
	}

	public int getTmin() {
		return tmin;
	}

	public void setTmin(int tmin) {
		this.tmin = tmin;
	}

	public boolean GetIsTmax() {
		return isTmax;
	}

	public void setIsTmax(boolean isTmax) {
		this.isTmax = isTmax;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(tmax);
		out.writeInt(tmin);
		out.writeBoolean(isTmax);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.tmax = in.readInt();
		this.tmin = in.readInt();
		this.isTmax = in.readBoolean();
	}
	
}
