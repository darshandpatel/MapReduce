package code;

public class Info {
	
	private float tminSum;
	private float tminCount;
	private float tmaxSum;
	private float tmaxCount;
	
	Info(float tminSum, float tminCount, float tmaxSum, float tmaxCount){
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

	public float getTminCount() {
		return tminCount;
	}

	public void setTminCount(float tminCount) {
		this.tminCount = tminCount;
	}

	public float getTmaxSum() {
		return tmaxSum;
	}

	public void setTmaxSum(float tmaxSum) {
		this.tmaxSum = tmaxSum;
	}

	public float getTmaxCount() {
		return tmaxCount;
	}

	public void setTmaxCount(float tmaxCount) {
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
