package in.ac.iitkgp.acaddwh.dao.fact;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import in.ac.iitkgp.acaddwh.bean.fact.SplPerformance;

public class SplPerformanceDAO {
	public int addToDB(Connection con, SplPerformance splPerformance) throws SQLException {
		int returnValue = 0;
		PreparedStatement ps = null;

		try {
			ps = con.prepareStatement("insert into acaddwh.fact_spl_performance values (?,?,?,?,?,?,?,?,?)");
			ps.setString(1, splPerformance.getInstituteKey());
			ps.setString(2, splPerformance.getSplKey());
			ps.setString(3, splPerformance.getTimeKey());
			ps.setInt(4, splPerformance.getAdmStuCnt());
			ps.setInt(5, splPerformance.getOnrollStuCnt());
			ps.setInt(6, splPerformance.getGradStuCnt());
			ps.setInt(7, splPerformance.getDropoutStuCnt());
			ps.setInt(8, splPerformance.getPercentPlaced());
			ps.setFloat(9, splPerformance.getAvgSalary());

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
			ps = con.prepareStatement("LOAD DATA LOCAL INPATH ? INTO TABLE acaddwh.fact_spl_performance");
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
