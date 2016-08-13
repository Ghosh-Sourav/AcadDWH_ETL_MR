package in.ac.iitkgp.acaddwh.dao.fact;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import in.ac.iitkgp.acaddwh.bean.fact.SemPerformance;

public class SemPerformanceDAO {
	public int addToDB(Connection con, SemPerformance semPerformance) throws SQLException {
		int returnValue = 0;
		PreparedStatement ps = null;

		try {
			ps = con.prepareStatement("insert into acaddwh.fact_sem_performance values (?,?,?,?,?,?,?)");
			ps.setString(1, semPerformance.getInstituteKey());
			ps.setString(2, semPerformance.getSplKey());
			ps.setString(3, semPerformance.getStudentKey());
			ps.setFloat(4, semPerformance.getCgpa());
			ps.setInt(5, semPerformance.getCourseRegistered());
			ps.setInt(6, semPerformance.getCreditRegistered());
			ps.setInt(7, semPerformance.getCourseFailed());

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
			ps = con.prepareStatement("LOAD DATA LOCAL INPATH ? INTO TABLE acaddwh.fact_sem_performance");
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
