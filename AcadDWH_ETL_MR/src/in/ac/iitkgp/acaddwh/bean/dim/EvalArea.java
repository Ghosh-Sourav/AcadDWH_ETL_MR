package in.ac.iitkgp.acaddwh.bean.dim;

import in.ac.iitkgp.acaddwh.bean.Item;

public class EvalArea extends Item {

	private String evalAreaKey;
	private String evalAreaCode;
	private String evalArea;

	public String getEvalAreaKey() {
		return evalAreaKey;
	}

	public void setEvalAreaKey(String evalAreaKey) {
		this.evalAreaKey = evalAreaKey;
	}

	public String getEvalAreaCode() {
		return evalAreaCode;
	}

	public void setEvalAreaCode(String evalAreaCode) {
		this.evalAreaCode = evalAreaCode;
	}

	public String getEvalArea() {
		return evalArea;
	}

	public void setEvalArea(String evalArea) {
		this.evalArea = evalArea;
	}

	@Override
	public String getPrintableLine() {
		String line;
		line = evalAreaKey + "," + evalAreaCode + "," + evalArea + "\n";
		return line;
	}

}
