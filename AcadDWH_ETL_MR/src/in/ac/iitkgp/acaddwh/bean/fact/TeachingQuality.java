package in.ac.iitkgp.acaddwh.bean.fact;

import in.ac.iitkgp.acaddwh.bean.Item;

public class TeachingQuality extends Item {

	private String instituteKey;
	private String courseKey;
	private String timeKey;
	private String teacherKey;
	private String evalAreaKey;
	private int noOfEvaluation;
	private float avgTeachingQuality;

	public String getInstituteKey() {
		return instituteKey;
	}

	public void setInstituteKey(String instituteKey) {
		this.instituteKey = instituteKey;
	}

	public String getCourseKey() {
		return courseKey;
	}

	public void setCourseKey(String courseKey) {
		this.courseKey = courseKey;
	}

	public String getTimeKey() {
		return timeKey;
	}

	public void setTimeKey(String timeKey) {
		this.timeKey = timeKey;
	}

	public String getTeacherKey() {
		return teacherKey;
	}

	public void setTeacherKey(String teacherKey) {
		this.teacherKey = teacherKey;
	}

	public String getEvalAreaKey() {
		return evalAreaKey;
	}

	public void setEvalAreaKey(String evalAreaKey) {
		this.evalAreaKey = evalAreaKey;
	}

	public int getNoOfEvaluation() {
		return noOfEvaluation;
	}

	public void setNoOfEvaluation(int noOfEvaluation) {
		this.noOfEvaluation = noOfEvaluation;
	}

	public float getAvgTeachingQuality() {
		return avgTeachingQuality;
	}

	public void setAvgTeachingQuality(float avgTeachingQuality) {
		this.avgTeachingQuality = avgTeachingQuality;
	}

	@Override
	public String getPrintableLine() {
		String line;
		line = instituteKey + "," + courseKey + "," + timeKey + "," + teacherKey + "," + evalAreaKey + ","
				+ noOfEvaluation + "," + avgTeachingQuality + "\n";
		return line;
	}

}
