package in.ac.iitkgp.acaddwh.dao.dim;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import in.ac.iitkgp.acaddwh.bean.dim.Institute;

public class AccountDAO {
	public Institute getInstitute(Connection con, String key, String password) throws SQLException {
		Institute institute = null;
		ResultSet rs = null;
		PreparedStatement ps = null;
		
		try {
			ps = con.prepareStatement(
					"select institute_name from acaddwh.dim_institute where institute_key = ? and institute_password = ?");
			ps.setString(1, key);
			ps.setString(2, password);

			rs = ps.executeQuery();
			if (rs.next()) {
				institute = new Institute();
				institute.setInstituteKey(key);
				institute.setInstituteName(rs.getString("institute_name"));
			}

		} finally {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
		}
		return institute;
	}
	
	public Institute getInstitute(Connection con, String key) throws SQLException {
		Institute institute = null;
		ResultSet rs = null;
		PreparedStatement ps = null;

		try {
			ps = con.prepareStatement(
					"select institute_name from acaddwh.dim_institute where institute_key = ?");
			ps.setString(1, key);

			rs = ps.executeQuery();
			if (rs.next()) {
				institute = new Institute();
				institute.setInstituteKey(key);
				institute.setInstituteName(rs.getString("institute_name"));
			}

		} finally {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
		}
		return institute;
	}
}
