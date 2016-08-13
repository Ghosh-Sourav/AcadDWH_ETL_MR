package in.ac.iitkgp.acaddwh.dao.fact;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import in.ac.iitkgp.acaddwh.bean.fact.StuLearning;

public class StuLearningDAO {
	public int addToDB(Connection con, StuLearning stuLearning) throws SQLException {
		int returnValue = 0;
		PreparedStatement ps = null;

		try {
			ps = con.prepareStatement("insert into acaddwh.fact_stu_learning values (?,?,?,?,?,?,?,?,?,?,?)");
			ps.setString(1, stuLearning.getInstituteKey());
			ps.setString(2, stuLearning.getCourseKey());
			ps.setString(3, stuLearning.getTimeKey());
			ps.setString(4, stuLearning.getStudentKey());
			ps.setString(5, stuLearning.getRegtypeKey());
			ps.setString(6, stuLearning.getGrade());
			ps.setInt(7, stuLearning.getNumGrade());
			ps.setFloat(8, stuLearning.getImprFactor());
			ps.setFloat(9, stuLearning.getPrFnImpr());
			ps.setFloat(10, stuLearning.getPrPsImpr());
			ps.setFloat(11, stuLearning.getPercentAttended());

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
			ps = con.prepareStatement("LOAD DATA LOCAL INPATH ? INTO TABLE acaddwh.fact_stu_learning");
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
