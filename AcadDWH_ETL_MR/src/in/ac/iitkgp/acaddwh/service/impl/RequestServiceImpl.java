package in.ac.iitkgp.acaddwh.service.impl;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import in.ac.iitkgp.acaddwh.bean.dim.Institute;
import in.ac.iitkgp.acaddwh.bean.dim.Request;
import in.ac.iitkgp.acaddwh.dao.RequestDAO;
import in.ac.iitkgp.acaddwh.service.RequestService;
import in.ac.iitkgp.acaddwh.util.DBConnection;

public class RequestServiceImpl implements RequestService {

	@Override
	public int addLog(Request request) {
		int returnValue = 0;

		Connection con = DBConnection.getReadConnection();
		RequestDAO requestDAO = new RequestDAO();

		try {
			returnValue = requestDAO.addLog(con, request);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			DBConnection.closeConnection(con);
		}

		return returnValue;
	}

	@Override
	public int updateLog(Request request) {
		int returnValue = 0;

		Connection con = DBConnection.getReadConnection();
		RequestDAO requestDAO = new RequestDAO();

		try {
			returnValue = requestDAO.updateLog(con, request);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			DBConnection.closeConnection(con);
		}

		return returnValue;
	}

	@Override
	public List<Request> getLogs(Institute institute) {
		List<Request> requests = new ArrayList<Request>();

		Connection con = DBConnection.getReadConnection();
		RequestDAO requestDAO = new RequestDAO();

		try {
			requests = requestDAO.getLogs(con, institute);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			DBConnection.closeConnection(con);
		}

		return requests;
	}
	
	@Override
	public List<Request> getLogs() {
		List<Request> requests = new ArrayList<Request>();

		Connection con = DBConnection.getReadConnection();
		RequestDAO requestDAO = new RequestDAO();

		try {
			requests = requestDAO.getLogs(con);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			DBConnection.closeConnection(con);
		}

		return requests;
	}

}
