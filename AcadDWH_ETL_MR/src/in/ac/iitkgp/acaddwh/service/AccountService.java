package in.ac.iitkgp.acaddwh.service;

import in.ac.iitkgp.acaddwh.bean.dim.Institute;
import in.ac.iitkgp.acaddwh.exception.ETLException;

public interface AccountService {
	public boolean instituteExists(String key, String password);

	public String getInstituteName(String key);
	
	public String signUpInstitute(Institute institute) throws ETLException;

}