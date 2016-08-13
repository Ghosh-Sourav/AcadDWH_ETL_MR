package in.ac.iitkgp.acaddwh.dao.dim;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import in.ac.iitkgp.acaddwh.bean.dim.Course;

public class CourseDAO {
	public int addToDB(Connection con, Course course) throws SQLException {
		int returnValue = 0;
		PreparedStatement ps = null;

		try {
			ps = con.prepareStatement("insert into acaddwh.dim_courses values (?,?,?,?,?,?,?,?,?,?)");
			ps.setString(1, course.getCourseKey());
			ps.setString(2, course.getCourseCode());
			ps.setString(3, course.getCourseName());
			ps.setString(4, course.getCourseType());
			ps.setString(5, course.getCourseDept());
			ps.setInt(6, course.getCourseCrd());
			ps.setInt(7, course.getCourseLectureHour());
			ps.setInt(8, course.getCourseTutorialHour());
			ps.setInt(9, course.getCoursePracticalHour());
			ps.setString(10, course.getCourseLevel());

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
			ps = con.prepareStatement("LOAD DATA LOCAL INPATH ? INTO TABLE acaddwh.dim_courses");
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
