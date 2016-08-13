package in.ac.iitkgp.acaddwh.bean.dim;

import in.ac.iitkgp.acaddwh.bean.Item;

public class Teacher extends Item {

	private String teacherKey;
	private String teacherCode;
	private String teacherDept;
	private String teacherDesg;

	public String getTeacherKey() {
		return teacherKey;
	}

	public void setTeacherKey(String teacherKey) {
		this.teacherKey = teacherKey;
	}

	public String getTeacherCode() {
		return teacherCode;
	}

	public void setTeacherCode(String teacherCode) {
		this.teacherCode = teacherCode;
	}

	public String getTeacherDept() {
		return teacherDept;
	}

	public void setTeacherDept(String teacherDept) {
		this.teacherDept = teacherDept;
	}

	public String getTeacherDesg() {
		return teacherDesg;
	}

	public void setTeacherDesg(String teacherDesg) {
		this.teacherDesg = teacherDesg;
	}

	@Override
	public String getPrintableLine() {
		String line;
		line = teacherKey + "," + teacherCode + "," + teacherDept + "," + teacherDesg + "\n";
		return line;
	}

}
