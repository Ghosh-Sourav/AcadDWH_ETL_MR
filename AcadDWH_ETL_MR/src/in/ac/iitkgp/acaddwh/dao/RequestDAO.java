package in.ac.iitkgp.acaddwh.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import in.ac.iitkgp.acaddwh.bean.dim.Institute;
import in.ac.iitkgp.acaddwh.bean.dim.Request;

public class RequestDAO {
	public int addLog(Connection con, Request request) throws SQLException {
		int returnValue = 0;
		PreparedStatement ps = null;

		try {
			ps = con.prepareStatement("insert into acaddwh.log_requests values (?,?,?,?)");
			ps.setString(1, request.getRequestKey());
			ps.setString(2, request.getInstituteKey());
			ps.setString(3, request.getFileNameWithoutExtn());
			ps.setString(4, request.getStatus());

			returnValue = ps.executeUpdate();

		} finally {
			if (ps != null) {
				ps.close();
			}
		}
		return returnValue;
	}
	
	public int updateLog(Connection con, Request request) throws SQLException {
		int returnValue = 0;
		PreparedStatement ps = null;

		try {
			ps = con.prepareStatement("update acaddwh.log_requests set status = ? where request_key = ?");
			ps.setString(1, request.getStatus());
			ps.setString(2, request.getRequestKey());

			returnValue = ps.executeUpdate();

		} finally {
			if (ps != null) {
				ps.close();
			}
		}
		return returnValue;
	}
	
	public List<Request> getLogs(Connection con, Institute institute) throws SQLException {
		List<Request> requests = new ArrayList<Request>();
		ResultSet rs = null;
		PreparedStatement ps = null;

		try {
			ps = con.prepareStatement(
					"select request_key, file_name_without_extn, status from acaddwh.log_requests where institute_key = ? order by request_key desc");
			ps.setString(1, institute.getInstituteKey());

			rs = ps.executeQuery();
			while (rs.next()) {
				Request request = new Request();
				request.setRequestKey(rs.getString("request_key"));
				request.setInstituteKey(institute.getInstituteKey());
				request.setFileNameWithoutExtn(rs.getString("file_name_without_extn"));
				request.setStatus(rs.getString("status"));
				
				requests.add(request);
			}

		} finally {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
		}
		
		return requests;
		
	}
	
	public List<Request> getLogs(Connection con) throws SQLException {
		List<Request> requests = new ArrayList<Request>();
		ResultSet rs = null;
		PreparedStatement ps = null;

		try {
			ps = con.prepareStatement(
					"select request_key, institute_key, file_name_without_extn, status from acaddwh.log_requests order by request_key desc");

			rs = ps.executeQuery();
			while (rs.next()) {
				Request request = new Request();
				request.setRequestKey(rs.getString("request_key"));
				request.setInstituteKey(rs.getString("institute_key"));
				request.setFileNameWithoutExtn(rs.getString("file_name_without_extn"));
				request.setStatus(rs.getString("status"));
				
				requests.add(request);
			}

		} finally {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
		}
		
		return requests;
		
	}
	
	

}
