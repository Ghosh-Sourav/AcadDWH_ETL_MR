package in.ac.iitkgp.acaddwh.bean.fact;

import in.ac.iitkgp.acaddwh.bean.Item;

public class SemPerformance extends Item {

	private String instituteKey;
	private String splKey;
	private String studentKey;
	private float cgpa;
	private int courseRegistered;
	private int creditRegistered;
	private int courseFailed;

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

	public String getStudentKey() {
		return studentKey;
	}

	public void setStudentKey(String studentKey) {
		this.studentKey = studentKey;
	}

	public float getCgpa() {
		return cgpa;
	}

	public void setCgpa(float cgpa) {
		this.cgpa = cgpa;
	}

	public int getCourseRegistered() {
		return courseRegistered;
	}

	public void setCourseRegistered(int courseRegistered) {
		this.courseRegistered = courseRegistered;
	}

	public int getCreditRegistered() {
		return creditRegistered;
	}

	public void setCreditRegistered(int creditRegistered) {
		this.creditRegistered = creditRegistered;
	}

	public int getCourseFailed() {
		return courseFailed;
	}

	public void setCourseFailed(int courseFailed) {
		this.courseFailed = courseFailed;
	}

	@Override
	public String getPrintableLine() {
		String line;
		line = instituteKey + "," + splKey + "," + studentKey + "," + cgpa + "," + courseRegistered + ","
				+ creditRegistered + "," + courseFailed + "\n";
		return line;
	}

}
