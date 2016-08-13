package in.ac.iitkgp.acaddwh.bean.fact;

import in.ac.iitkgp.acaddwh.bean.Item;

public class StuLearning extends Item {

	private String instituteKey;
	private String courseKey;
	private String timeKey;
	private String studentKey;
	private String regtypeKey;
	private String grade;
	private int numGrade;
	private float imprFactor;
	private float prFnImpr;
	private float prPsImpr;
	private float percentAttended;

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

	public String getStudentKey() {
		return studentKey;
	}

	public void setStudentKey(String studentKey) {
		this.studentKey = studentKey;
	}

	public String getRegtypeKey() {
		return regtypeKey;
	}

	public void setRegtypeKey(String regtypeKey) {
		this.regtypeKey = regtypeKey;
	}

	public String getGrade() {
		return grade;
	}

	public void setGrade(String grade) {
		this.grade = grade;
	}

	public int getNumGrade() {
		return numGrade;
	}

	public void setNumGrade(int numGrade) {
		this.numGrade = numGrade;
	}

	public float getImprFactor() {
		return imprFactor;
	}

	public void setImprFactor(float imprFactor) {
		this.imprFactor = imprFactor;
	}

	public float getPrFnImpr() {
		return prFnImpr;
	}

	public void setPrFnImpr(float prFnImpr) {
		this.prFnImpr = prFnImpr;
	}

	public float getPrPsImpr() {
		return prPsImpr;
	}

	public void setPrPsImpr(float prPsImpr) {
		this.prPsImpr = prPsImpr;
	}

	public float getPercentAttended() {
		return percentAttended;
	}

	public void setPercentAttended(float percentAttended) {
		this.percentAttended = percentAttended;
	}

	@Override
	public String getPrintableLine() {
		String line;
		line = instituteKey + "," + courseKey + "," + timeKey + "," + studentKey + "," + regtypeKey + "," + grade + ","
				+ numGrade + "," + imprFactor + "," + prFnImpr + "," + prPsImpr + "," + percentAttended + "\n";
		return line;
	}

}
