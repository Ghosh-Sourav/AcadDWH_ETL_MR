package in.ac.iitkgp.acaddwh.bean.dim;

import in.ac.iitkgp.acaddwh.bean.Item;

public class Time extends Item {

	private String timeKey;
	private String timeCode;
	private String acadsemester;
	private String acadsession;

	public String getTimeKey() {
		return timeKey;
	}

	public void setTimeKey(String timeKey) {
		this.timeKey = timeKey;
	}

	public String getTimeCode() {
		return timeCode;
	}

	public void setTimeCode(String timeCode) {
		this.timeCode = timeCode;
	}

	public String getAcadsemester() {
		return acadsemester;
	}

	public void setAcadsemester(String acadsemester) {
		this.acadsemester = acadsemester;
	}

	public String getAcadsession() {
		return acadsession;
	}

	public void setAcadsession(String acadsession) {
		this.acadsession = acadsession;
	}

	@Override
	public String getPrintableLine() {
		String line;
		line = timeKey + "," + timeCode + "," + acadsemester + "," + acadsession + "\n";
		return line;
	}

}
