package in.ac.iitkgp.acaddwh.bean.dim;

import in.ac.iitkgp.acaddwh.bean.Item;

public class Student extends Item {
	private String studentKey;
	private String studentCode;
	private String splKey;
	private String studentNoa;
	private String studentGender;
	private int admissionYear;

	public String getStudentKey() {
		return studentKey;
	}

	public void setStudentKey(String studentKey) {
		this.studentKey = studentKey;
	}

	public String getStudentCode() {
		return studentCode;
	}

	public void setStudentCode(String studentCode) {
		this.studentCode = studentCode;
	}

	public String getSplKey() {
		return splKey;
	}

	public void setSplKey(String splKey) {
		this.splKey = splKey;
	}

	public String getStudentNoa() {
		return studentNoa;
	}

	public void setStudentNoa(String studentNoa) {
		this.studentNoa = studentNoa;
	}

	public String getStudentGender() {
		return studentGender;
	}

	public void setStudentGender(String studentGender) {
		this.studentGender = studentGender;
	}

	public int getAdmissionYear() {
		return admissionYear;
	}

	public void setAdmissionYear(int admissionYear) {
		this.admissionYear = admissionYear;
	}

	@Override
	public String toString() {
		return "<Key = " + studentKey + ", Value = {" + studentCode + "," + splKey + "," + studentNoa + ","
				+ studentGender + "," + admissionYear + "}";
	}

	@Override
	public String getPrintableLine() {
		String line;
		line = studentKey + "," + studentCode + "," + splKey + "," + studentNoa + "," + studentGender + ","
				+ admissionYear + "\n";
		return line;
	}

}
