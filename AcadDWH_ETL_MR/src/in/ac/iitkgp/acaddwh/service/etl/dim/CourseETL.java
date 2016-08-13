package in.ac.iitkgp.acaddwh.service.etl.dim;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import in.ac.iitkgp.acaddwh.bean.dim.Course;
import in.ac.iitkgp.acaddwh.dao.dim.CourseDAO;
import in.ac.iitkgp.acaddwh.exception.ExtractException;
import in.ac.iitkgp.acaddwh.exception.LoadException;
import in.ac.iitkgp.acaddwh.exception.TransformException;
import in.ac.iitkgp.acaddwh.exception.WarehouseException;
import in.ac.iitkgp.acaddwh.service.ETLService;
import in.ac.iitkgp.acaddwh.util.DBConnection;
import in.ac.iitkgp.acaddwh.util.HiveConnection;
import in.ac.iitkgp.acaddwh.util.LogFile;

public class CourseETL implements ETLService<Course> {

	public List<?> extract(String filePath, String splitter, String absoluteLogFileName) throws ExtractException {
		List<Course> courses = new ArrayList<Course>();
		StringBuffer logString = new StringBuffer();
		BufferedReader br = null;
		String line = null;

		try {
			br = new BufferedReader(new FileReader(filePath));
			while ((line = br.readLine()) != null) {
				Course course = new Course();
				String[] values = line.split(splitter);

				course.setCourseCode(values[0]);
				course.setCourseName(values[1]);
				course.setCourseType(values[2]);
				course.setCourseDept(values[3]);
				course.setCourseCrd(Integer.parseInt(values[4]));
				course.setCourseLectureHour(Integer.parseInt(values[5]));
				course.setCourseTutorialHour(Integer.parseInt(values[6]));
				course.setCoursePracticalHour(Integer.parseInt(values[7]));
				course.setCourseLevel(values[8]);

				System.out.println("Extracted Course " + course);

				courses.add(course);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logString.append(
					"Extract," + courses.size() + ",Not Extracted,Data format is invalid - Further lines ignored\n");
			LogFile.writeToLogFile(absoluteLogFileName, logString);
			throw (new ExtractException());
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return courses;
	}

	@SuppressWarnings("unchecked")
	public int transform(List<?> courses, String instituteCode, String absoluteLogFileName) throws TransformException {
		int count = 0;
		try {
			for (Course course : (List<Course>) courses) {
				course.setCourseKey(instituteCode + '_' + course.getCourseCode());
				System.out.println("Transformed Course " + course);
				count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw (new TransformException());
		}
		return count;
	}

	@SuppressWarnings("unchecked")
	public int load(List<?> courses, String absoluteLogFileName) throws LoadException {
		int count = 0, processedLineCount = 0;
		StringBuffer logString = new StringBuffer();

		Connection con = DBConnection.getWriteConnection();
		CourseDAO courseDAO = new CourseDAO();

		try {
			for (Course course : (List<Course>) courses) {
				try {
					++processedLineCount;
					count += courseDAO.addToDB(con, course);
					System.out.println("[UC] Loaded Course " + course);
				} catch (SQLException e) {
					logString.append("Load," + processedLineCount + "," + course.getCourseCode() + ","
							+ LogFile.getErrorMsg(e) + "\n");
					con.rollback();
				}
			}
			if (logString.length() != 0) {
				throw (new LoadException());
			}
			System.out.println("Committing updates...");
			con.commit();
		} catch (Exception e) {
			try {
				System.out.println("Rolling back changes...");
				con.rollback();
				LogFile.writeToLogFile(absoluteLogFileName, logString);
				count = 0;
				throw (new LoadException());
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		} finally {
			DBConnection.closeConnection(con);
		}

		return count;
	}

	public void warehouse(String hadoopLocalFileName, String absoluteLogFileName) throws WarehouseException {
		StringBuffer logString = new StringBuffer();

		Connection con = HiveConnection.getSaveConnection();
		CourseDAO courseDAO = new CourseDAO();

		try {
			courseDAO.addToHive(con, hadoopLocalFileName);
			System.out.println("[W] Warehoused Course file: " + hadoopLocalFileName);

		} catch (SQLException e) {
			System.out.println("WarehouseException thrown!");
			logString.append("Warehouse," + "-" + "," + "-" + "," + LogFile.getErrorMsg(e) + "\n");
			LogFile.writeToLogFile(absoluteLogFileName, logString);
			throw (new WarehouseException());
		} finally {
			HiveConnection.closeConnection(con);
		}
	}
}
