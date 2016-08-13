package in.ac.iitkgp.acaddwh.bean.dim;

import in.ac.iitkgp.acaddwh.bean.Item;

public class Regtype extends Item {

	private String regtypeKey;
	private String regtypeCode;
	private String regtypeDesc;

	public String getRegtypeKey() {
		return regtypeKey;
	}

	public void setRegtypeKey(String regtypeKey) {
		this.regtypeKey = regtypeKey;
	}

	public String getRegtypeCode() {
		return regtypeCode;
	}

	public void setRegtypeCode(String regtypeCode) {
		this.regtypeCode = regtypeCode;
	}

	public String getRegtypeDesc() {
		return regtypeDesc;
	}

	public void setRegtypeDesc(String regtypeDesc) {
		this.regtypeDesc = regtypeDesc;
	}

	@Override
	public String getPrintableLine() {
		String line;
		line = regtypeKey + "," + regtypeCode + "," + regtypeDesc + "\n";
		return line;
	}

}
