package in.ac.iitkgp.acaddwh.bean.dim;

import in.ac.iitkgp.acaddwh.bean.Item;

public class Course extends Item {

	private String courseKey;
	private String courseCode;
	private String courseName;
	private String courseType;
	private String courseDept;
	private int courseCrd;
	private int courseLectureHour;
	private int courseTutorialHour;
	private int coursePracticalHour;
	private String courseLevel;

	public String getCourseKey() {
		return courseKey;
	}

	public void setCourseKey(String courseKey) {
		this.courseKey = courseKey;
	}

	public String getCourseCode() {
		return courseCode;
	}

	public void setCourseCode(String courseCode) {
		this.courseCode = courseCode;
	}

	public String getCourseName() {
		return courseName;
	}

	public void setCourseName(String courseName) {
		this.courseName = courseName;
	}

	public String getCourseType() {
		return courseType;
	}

	public void setCourseType(String courseType) {
		this.courseType = courseType;
	}

	public String getCourseDept() {
		return courseDept;
	}

	public void setCourseDept(String courseDept) {
		this.courseDept = courseDept;
	}

	public int getCourseCrd() {
		return courseCrd;
	}

	public void setCourseCrd(int courseCrd) {
		this.courseCrd = courseCrd;
	}

	public int getCourseLectureHour() {
		return courseLectureHour;
	}

	public void setCourseLectureHour(int courseLectureHour) {
		this.courseLectureHour = courseLectureHour;
	}

	public int getCourseTutorialHour() {
		return courseTutorialHour;
	}

	public void setCourseTutorialHour(int courseTutorialHour) {
		this.courseTutorialHour = courseTutorialHour;
	}

	public int getCoursePracticalHour() {
		return coursePracticalHour;
	}

	public void setCoursePracticalHour(int coursePracticalHour) {
		this.coursePracticalHour = coursePracticalHour;
	}

	public String getCourseLevel() {
		return courseLevel;
	}

	public void setCourseLevel(String courseLevel) {
		this.courseLevel = courseLevel;
	}

	@Override
	public String getPrintableLine() {
		String line;
		line = courseKey + "," + courseCode + "," + courseName + "," + courseType + "," + courseDept + "," + courseCrd
				+ "," + courseLectureHour + "," + courseTutorialHour + "," + coursePracticalHour + "," + courseLevel
				+ "\n";
		return line;
	}

}
