package in.ac.iitkgp.acaddwh.service.etl.dim;

import java.util.ArrayList;
import java.util.List;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import in.ac.iitkgp.acaddwh.bean.dim.Student;
import in.ac.iitkgp.acaddwh.dao.dim.StudentDAO;
import in.ac.iitkgp.acaddwh.exception.ExtractException;
import in.ac.iitkgp.acaddwh.exception.LoadException;
import in.ac.iitkgp.acaddwh.exception.TransformException;
import in.ac.iitkgp.acaddwh.exception.WarehouseException;
import in.ac.iitkgp.acaddwh.service.ETLService;
import in.ac.iitkgp.acaddwh.util.DBConnection;
import in.ac.iitkgp.acaddwh.util.HiveConnection;
import in.ac.iitkgp.acaddwh.util.LogFile;

public class StudentETL implements ETLService<Student> {

	public List<?> extract(String filePath, String splitter, String absoluteLogFileName) throws ExtractException {
		List<Student> students = new ArrayList<Student>();
		StringBuffer logString = new StringBuffer();
		BufferedReader br = null;
		String line = null;

		try {
			br = new BufferedReader(new FileReader(filePath));
			while ((line = br.readLine()) != null) {
				Student student = new Student();
				String[] values = line.split(splitter);

				student.setStudentCode(values[0]);
				student.setSplKey(values[1]);
				student.setStudentNoa(values[2]);
				student.setStudentGender(values[3]);
				student.setAdmissionYear(Integer.parseInt(values[4]));

				System.out.println("Extracted Student " + student);

				students.add(student);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logString.append(
					"Extract," + students.size() + ",Not Extracted,Data format is invalid - Further lines ignored\n");
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

		return students;
	}

	@SuppressWarnings("unchecked")
	public int transform(List<?> students, String instituteCode, String absoluteLogFileName) throws TransformException {
		int count = 0;
		try {
			for (Student student : (List<Student>) students) {
				student.setStudentKey(instituteCode + '_' + student.getStudentCode());
				student.setSplKey(instituteCode + '_' + student.getSplKey());
				System.out.println("Transformed Student " + student);
				count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw (new TransformException());
		}
		return count;
	}

	@SuppressWarnings("unchecked")
	public int load(List<?> students, String absoluteLogFileName) throws LoadException {
		int count = 0, processedLineCount = 0;
		StringBuffer logString = new StringBuffer();

		Connection con = DBConnection.getWriteConnection();
		StudentDAO studentDAO = new StudentDAO();

		try {
			for (Student student : (List<Student>) students) {
				try {
					++processedLineCount;
					count += studentDAO.addToDB(con, student);
					System.out.println("[UC] Loaded Student " + student);
				} catch (SQLException e) {
					logString.append("Load," + processedLineCount + "," + student.getStudentCode() + ","
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
		StudentDAO studentDAO = new StudentDAO();

		try {
			studentDAO.addToHive(con, hadoopLocalFileName);
			System.out.println("[W] Warehoused Student file: " + hadoopLocalFileName);

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
