package in.ac.iitkgp.acaddwh.bean.dim;

import in.ac.iitkgp.acaddwh.bean.Item;

public class Department extends Item {

	private String deptKey;
	private String deptCode;
	private String deptName;
	private String deptDcsType;

	public String getDeptKey() {
		return deptKey;
	}

	public void setDeptKey(String deptKey) {
		this.deptKey = deptKey;
	}

	public String getDeptCode() {
		return deptCode;
	}

	public void setDeptCode(String deptCode) {
		this.deptCode = deptCode;
	}

	public String getDeptName() {
		return deptName;
	}

	public void setDeptName(String deptName) {
		this.deptName = deptName;
	}

	public String getDeptDcsType() {
		return deptDcsType;
	}

	public void setDeptDcsType(String deptDcsType) {
		this.deptDcsType = deptDcsType;
	}

	@Override
	public String getPrintableLine() {
		String line;
		line = deptKey + "," + deptCode + "," + deptName + "," + deptDcsType + "\n";
		return line;
	}

}
