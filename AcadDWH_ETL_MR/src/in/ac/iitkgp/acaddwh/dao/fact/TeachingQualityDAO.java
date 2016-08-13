package in.ac.iitkgp.acaddwh.dao.fact;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import in.ac.iitkgp.acaddwh.bean.fact.TeachingQuality;

public class TeachingQualityDAO {
	public int addToDB(Connection con, TeachingQuality teachingQuality) throws SQLException {
		int returnValue = 0;
		PreparedStatement ps = null;

		try {
			ps = con.prepareStatement("insert into acaddwh.fact_teaching_quality values (?,?,?,?,?,?,?)");
			ps.setString(1, teachingQuality.getInstituteKey());
			ps.setString(2, teachingQuality.getCourseKey());
			ps.setString(3, teachingQuality.getTimeKey());
			ps.setString(4, teachingQuality.getTeacherKey());
			ps.setString(5, teachingQuality.getEvalAreaKey());
			ps.setInt(6, teachingQuality.getNoOfEvaluation());
			ps.setFloat(7, teachingQuality.getAvgTeachingQuality());

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
			ps = con.prepareStatement("LOAD DATA LOCAL INPATH ? INTO TABLE acaddwh.fact_teaching_quality");
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
