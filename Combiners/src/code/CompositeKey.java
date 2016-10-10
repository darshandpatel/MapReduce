package code;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class is composite key class for the secondary sort problem
 * Here station id and year is included in the composite key
 * @author Darshan
 *
 */
public class CompositeKey implements Writable, WritableComparable<CompositeKey>{
	
	private String stationId;
	private Integer year;
	
	public String getStationId(){
		return stationId;
	}
	
	public Integer getYear(){
		return year;
	}
	
	public void setStationId(String stationId){
		this.stationId = stationId;
	}
	
	public void setYear(Integer year){
		this.year = year;
	}
	
	@Override
	public int compareTo(CompositeKey key){
		int strComp = stationId.compareTo(key.getStationId());
		
		if(strComp == 0){
			return year.compareTo(key.getYear());
		}
		return strComp;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		stationId = in.readUTF();
		year = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(stationId);
		out.writeInt(year);
	}
}
