package in.ac.iitkgp.acaddwh.dao.dim;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import in.ac.iitkgp.acaddwh.bean.dim.Specialisation;

public class SpecialisationDAO {
	public int addToDB(Connection con, Specialisation specialisation) throws SQLException {
		int returnValue = 0;
		PreparedStatement ps = null;

		try {
			ps = con.prepareStatement("insert into acaddwh.dim_specialisations values (?,?,?,?,?,?)");
			ps.setString(1, specialisation.getSplKey());
			ps.setString(2, specialisation.getSplCode());
			ps.setString(3, specialisation.getSplName());
			ps.setString(4, specialisation.getDeptKey());
			ps.setString(5, specialisation.getSplDegree());
			ps.setString(6, specialisation.getSplLevel());

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
			ps = con.prepareStatement("LOAD DATA LOCAL INPATH ? INTO TABLE acaddwh.dim_specialisations");
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
