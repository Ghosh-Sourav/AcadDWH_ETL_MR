package in.ac.iitkgp.acaddwh.dao.dim;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import in.ac.iitkgp.acaddwh.bean.dim.Department;

public class DepartmentDAO {
	public int addToDB(Connection con, Department department) throws SQLException {
		int returnValue = 0;
		PreparedStatement ps = null;

		try {
			ps = con.prepareStatement("insert into acaddwh.dim_departments values (?,?,?,?)");
			ps.setString(1, department.getDeptKey());
			ps.setString(2, department.getDeptCode());
			ps.setString(3, department.getDeptName());
			ps.setString(4, department.getDeptDcsType());

			returnValue = ps.executeUpdate();

		} finally {
			if (ps != null) {
				ps.close();
			}
		}
		return returnValue;
	}

	public int addToHive(Connection con, String hadoopLocalFileName) throws SQLException {
		int returnValue = 0;
		PreparedStatement ps = null;

		try {
			ps = con.prepareStatement("LOAD DATA LOCAL INPATH ? INTO TABLE acaddwh.dim_departments");
			ps.setString(1, hadoopLocalFileName);
			
			returnValue = ps.executeUpdate();

		} finally {
			if (ps != null) {
				ps.close();
			}
		}
		return returnValue;
	}

}
