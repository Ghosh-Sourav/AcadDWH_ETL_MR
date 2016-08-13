package in.ac.iitkgp.acaddwh.bean.dim;

import in.ac.iitkgp.acaddwh.bean.Item;

public class Specialisation extends Item {

	private String splKey;
	private String splCode;
	private String splName;
	private String deptKey;
	private String splDegree;
	private String splLevel;

	public String getSplKey() {
		return splKey;
	}

	public void setSplKey(String splKey) {
		this.splKey = splKey;
	}

	public String getSplCode() {
		return splCode;
	}

	public void setSplCode(String splCode) {
		this.splCode = splCode;
	}

	public String getSplName() {
		return splName;
	}

	public void setSplName(String splName) {
		this.splName = splName;
	}

	public String getDeptKey() {
		return deptKey;
	}

	public void setDeptKey(String deptKey) {
		this.deptKey = deptKey;
	}

	public String getSplDegree() {
		return splDegree;
	}

	public void setSplDegree(String splDegree) {
		this.splDegree = splDegree;
	}

	public String getSplLevel() {
		return splLevel;
	}

	public void setSplLevel(String splLevel) {
		this.splLevel = splLevel;
	}

	@Override
	public String getPrintableLine() {
		String line;
		line = splKey + "," + splCode + "," + splName + "," + deptKey + "," + splDegree + "," + splLevel + "\n";
		return line;
	}

}
