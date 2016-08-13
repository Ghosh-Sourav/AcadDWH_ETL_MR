package in.ac.iitkgp.acaddwh.bean.dim;

import in.ac.iitkgp.acaddwh.bean.Item;

public class Institute extends Item {
	private String instituteKey;
	private String instituteName;
	private String institutePassword;
	// private String instituteCategory;
	// private String instituteLoc;
	// private String instituteState;
	// private String instituteCountry;

	public String getInstituteKey() {
		return instituteKey;
	}

	public void setInstituteKey(String instituteKey) {
		this.instituteKey = instituteKey;
	}

	public String getInstituteName() {
		return instituteName;
	}

	public void setInstituteName(String instituteName) {
		this.instituteName = instituteName;
	}

	public String getInstitutePassword() {
		return institutePassword;
	}

	public void setInstitutePassword(String institutePassword) {
		this.institutePassword = institutePassword;
	}

	@Override
	public String getPrintableLine() {
		String line;
		line = instituteKey + "," + instituteName + "," + institutePassword + "\n";
		return line;
	}

	// public String getInstituteCategory() {
	// return instituteCategory;
	// }
	//
	// public void setInstituteCategory(String instituteCategory) {
	// this.instituteCategory = instituteCategory;
	// }
	//
	// public String getInstituteLoc() {
	// return instituteLoc;
	// }
	//
	// public void setInstituteLoc(String instituteLoc) {
	// this.instituteLoc = instituteLoc;
	// }
	//
	// public String getInstituteState() {
	// return instituteState;
	// }
	//
	// public void setInstituteState(String instituteState) {
	// this.instituteState = instituteState;
	// }
	//
	// public String getInstituteCountry() {
	// return instituteCountry;
	// }
	//
	// public void setInstituteCountry(String instituteCountry) {
	// this.instituteCountry = instituteCountry;
	// }

}
