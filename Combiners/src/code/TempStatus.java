package code;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TempStatus implements Writable{

	int tmaxCount;
	int tminCount;
	float tmax;
	float tmin;
	int year;
	boolean isTmax;
	
	public TempStatus(){
		this.tmaxCount = 0;
		this.tmax = 0;
		this.tminCount = 0;
		this.tmin = 0;
		this.year = 0;
		this.isTmax = false;
	}
	
	public TempStatus(float tmax, int tmaxCount, float tmin, int tminCount){
		this.tmax = tmax;
		this.tmin = tmin;
		this.tmaxCount = tmaxCount;
		this.tminCount = tminCount;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeFloat(tmax);
		out.writeFloat(tmin);
		out.writeInt(tmaxCount);
		out.writeInt(tminCount);
		out.writeInt(year);
		out.writeBoolean(isTmax);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.tmax = in.readFloat();
		this.tmin = in.readFloat();
		this.tmaxCount = in.readInt();
		this.tminCount = in.readInt();
		this.year = in.readInt();
		this.isTmax = in.readBoolean();
	}

	public int getTmaxCount() {
		return tmaxCount;
	}

	public void setTmaxCount(int tmaxCount) {
		this.tmaxCount = tmaxCount;
	}

	public int getTminCount() {
		return tminCount;
	}

	public void setTminCount(int tminCount) {
		this.tminCount = tminCount;
	}

	public float getTmax() {
		return tmax;
	}

	public void setTmax(float tmax) {
		this.tmax = tmax;
	}

	public float getTmin() {
		return tmin;
	}

	public void setTmin(float tmin) {
		this.tmin = tmin;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public boolean isTmax() {
		return isTmax;
	}

	public void setIsTmax(boolean isTmax) {
		this.isTmax = isTmax;
	}
	
}
