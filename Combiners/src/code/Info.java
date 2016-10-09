package code;

public class Info {
	
	private float tminSum;
	private int tminCount;
	private float tmaxSum;
	private int tmaxCount;
	
	Info(float tminSum, int tminCount, float tmaxSum, int tmaxCount){
		this.tminSum = tminSum;
		this.tminCount = tminCount;
		this.tmaxSum = tmaxSum;
		this.tmaxCount = tmaxCount;
	}

	public float getTminSum() {
		return tminSum;
	}

	public void setTminSum(float tminSum) {
		this.tminSum = tminSum;
	}

	public int getTminCount() {
		return tminCount;
	}

	public void setTminCount(int tminCount) {
		this.tminCount = tminCount;
	}

	public float getTmaxSum() {
		return tmaxSum;
	}

	public void setTmaxSum(float tmaxSum) {
		this.tmaxSum = tmaxSum;
	}

	public int getTmaxCount() {
		return tmaxCount;
	}

	public void setTmaxCount(int tmaxCount) {
		this.tmaxCount = tmaxCount;
	}
	
	public void addTmax(float tmax){
		this.tmaxSum += tmax;
		this.tmaxCount += 1;
	}
	
	public void addTmin(float tmin){
		this.tminSum += tmin;
		this.tminCount += 1;
	}

}
