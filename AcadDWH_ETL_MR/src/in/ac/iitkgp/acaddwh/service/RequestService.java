package in.ac.iitkgp.acaddwh.service;

import java.util.List;

import in.ac.iitkgp.acaddwh.bean.dim.Institute;
import in.ac.iitkgp.acaddwh.bean.dim.Request;

public interface RequestService {
	public int addLog(Request request);
	
	public int updateLog(Request request);
	
	public List<Request> getLogs(Institute institute);
	
	public List<Request> getLogs();
}
