package in.ac.iitkgp.acaddwh.bean.fact;

import in.ac.iitkgp.acaddwh.bean.Item;

public class SplPerformance extends Item {

	private String instituteKey;
	private String splKey;
	private String timeKey;
	private int admStuCnt;
	private int onrollStuCnt;
	private int gradStuCnt;
	private int dropoutStuCnt;
	private int percentPlaced;
	private float avgSalary;

	public String getInstituteKey() {
		return instituteKey;
	}

	public void setInstituteKey(String instituteKey) {
		this.instituteKey = instituteKey;
	}

	public String getSplKey() {
		return splKey;
	}

	public void setSplKey(String splKey) {
		this.splKey = splKey;
	}

	public String getTimeKey() {
		return timeKey;
	}

	public void setTimeKey(String timeKey) {
		this.timeKey = timeKey;
	}

	public int getAdmStuCnt() {
		return admStuCnt;
	}

	public void setAdmStuCnt(int admStuCnt) {
		this.admStuCnt = admStuCnt;
	}

	public int getOnrollStuCnt() {
		return onrollStuCnt;
	}

	public void setOnrollStuCnt(int onrollStuCnt) {
		this.onrollStuCnt = onrollStuCnt;
	}

	public int getGradStuCnt() {
		return gradStuCnt;
	}

	public void setGradStuCnt(int gradStuCnt) {
		this.gradStuCnt = gradStuCnt;
	}

	public int getDropoutStuCnt() {
		return dropoutStuCnt;
	}

	public void setDropoutStuCnt(int dropoutStuCnt) {
		this.dropoutStuCnt = dropoutStuCnt;
	}

	public int getPercentPlaced() {
		return percentPlaced;
	}

	public void setPercentPlaced(int percentPlaced) {
		this.percentPlaced = percentPlaced;
	}

	public float getAvgSalary() {
		return avgSalary;
	}

	public void setAvgSalary(float avgSalary) {
		this.avgSalary = avgSalary;
	}

	@Override
	public String getPrintableLine() {
		String line;
		line = instituteKey + "," + splKey + "," + timeKey + "," + admStuCnt + "," + onrollStuCnt + "," + gradStuCnt
				+ "," + dropoutStuCnt + "," + percentPlaced + "," + avgSalary + "\n";
		return line;
	}

}
