package code;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TempStatus implements Writable{

	int year;
	float tmax;
	float tmin;
	int tmaxCount;
	int tminCount;
	boolean isTmax;
	
	public TempStatus(){
	}
	
	public TempStatus(float tmax, int tmaxCount, float tmin, int tminCount){
		this.tmax = tmax;
		this.tmin = tmin;
		this.tmaxCount = tmaxCount;
		this.tminCount = tminCount;
	}
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
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

	public boolean GetIsTmax() {
		return isTmax;
	}

	public void setIsTmax(boolean isTmax) {
		this.isTmax = isTmax;
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

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeFloat(tmax);
		out.writeFloat(tmin);
		out.writeInt(tminCount);
		out.writeInt(tmaxCount);
		out.writeBoolean(isTmax);
		out.writeInt(year);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.tmax = in.readFloat();
		this.tmin = in.readFloat();
		this.tmaxCount = in.readInt();
		this.tminCount = in.readInt();
		this.isTmax = in.readBoolean();
		this.year = in.readInt();
	}
	
	public static TempStatus read(DataInput in) throws IOException {
		TempStatus tempStatus = new TempStatus();
		tempStatus.readFields(in);
        return tempStatus;
      }
	
	public void addTmax(float tmax){
		this.tmax += tmax;
		this.tmaxCount += 1;
	}
	
	public void addTmin(float tmin){
		this.tmin += tmin;
		this.tminCount += 1;
	}
	
}
