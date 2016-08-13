package in.ac.iitkgp.acaddwh.service.etl.dim;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import in.ac.iitkgp.acaddwh.bean.dim.Teacher;
import in.ac.iitkgp.acaddwh.dao.dim.TeacherDAO;
import in.ac.iitkgp.acaddwh.exception.ExtractException;
import in.ac.iitkgp.acaddwh.exception.LoadException;
import in.ac.iitkgp.acaddwh.exception.TransformException;
import in.ac.iitkgp.acaddwh.exception.WarehouseException;
import in.ac.iitkgp.acaddwh.service.ETLService;
import in.ac.iitkgp.acaddwh.util.DBConnection;
import in.ac.iitkgp.acaddwh.util.HiveConnection;
import in.ac.iitkgp.acaddwh.util.LogFile;

public class TeacherETL implements ETLService<Teacher> {

	public List<?> extract(String filePath, String splitter, String absoluteLogFileName) throws ExtractException {
		List<Teacher> teachers = new ArrayList<Teacher>();
		StringBuffer logString = new StringBuffer();
		BufferedReader br = null;
		String line = null;

		try {
			br = new BufferedReader(new FileReader(filePath));
			while ((line = br.readLine()) != null) {
				Teacher teacher = new Teacher();
				String[] values = line.split(splitter);

				teacher.setTeacherCode(values[0]);
				teacher.setTeacherDept(values[1]);
				teacher.setTeacherDesg(values[2]);

				System.out.println("Extracted Teacher " + teacher);

				teachers.add(teacher);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logString.append(
					"Extract," + teachers.size() + ",Not Extracted,Data format is invalid - Further lines ignored\n");
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

		return teachers;
	}

	@SuppressWarnings("unchecked")
	public int transform(List<?> teachers, String instituteCode, String absoluteLogFileName) throws TransformException {
		int count = 0;
		try {
			for (Teacher teacher : (List<Teacher>) teachers) {
				teacher.setTeacherKey(instituteCode + '_' + teacher.getTeacherCode());
				System.out.println("Transformed Teacher " + teacher);
				count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw (new TransformException());
		}
		return count;
	}

	@SuppressWarnings("unchecked")
	public int load(List<?> teachers, String absoluteLogFileName) throws LoadException {
		int count = 0, processedLineCount = 0;
		StringBuffer logString = new StringBuffer();

		Connection con = DBConnection.getWriteConnection();
		TeacherDAO teacherDAO = new TeacherDAO();

		try {
			for (Teacher teacher : (List<Teacher>) teachers) {
				try {
					++processedLineCount;
					count += teacherDAO.addToDB(con, teacher);
					System.out.println("[UC] Loaded Teacher " + teacher);
				} catch (SQLException e) {
					logString.append("Load," + processedLineCount + "," + teacher.getTeacherCode() + ","
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
		TeacherDAO teacherDAO = new TeacherDAO();

		try {
			teacherDAO.addToHive(con, hadoopLocalFileName);
			System.out.println("[W] Warehoused Teacher file: " + hadoopLocalFileName);

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
