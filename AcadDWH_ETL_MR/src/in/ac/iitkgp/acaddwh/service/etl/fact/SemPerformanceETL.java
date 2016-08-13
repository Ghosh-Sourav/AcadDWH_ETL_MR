package in.ac.iitkgp.acaddwh.service.etl.fact;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import in.ac.iitkgp.acaddwh.bean.fact.SemPerformance;
import in.ac.iitkgp.acaddwh.dao.fact.SemPerformanceDAO;
import in.ac.iitkgp.acaddwh.exception.ExtractException;
import in.ac.iitkgp.acaddwh.exception.LoadException;
import in.ac.iitkgp.acaddwh.exception.TransformException;
import in.ac.iitkgp.acaddwh.exception.WarehouseException;
import in.ac.iitkgp.acaddwh.service.ETLService;
import in.ac.iitkgp.acaddwh.util.DBConnection;
import in.ac.iitkgp.acaddwh.util.HiveConnection;
import in.ac.iitkgp.acaddwh.util.LogFile;

public class SemPerformanceETL implements ETLService<SemPerformance> {

	public List<?> extract(String filePath, String splitter, String absoluteLogFileName) throws ExtractException {
		List<SemPerformance> semPerformances = new ArrayList<SemPerformance>();
		StringBuffer logString = new StringBuffer();
		BufferedReader br = null;
		String line = null;

		try {
			br = new BufferedReader(new FileReader(filePath));
			while ((line = br.readLine()) != null) {
				SemPerformance semPerformance = new SemPerformance();
				String[] values = line.split(splitter);

				semPerformance.setSplKey(values[0]);
				semPerformance.setStudentKey(values[1]);
				semPerformance.setCgpa(Float.parseFloat(values[2]));
				semPerformance.setCourseRegistered(Integer.parseInt(values[3]));
				semPerformance.setCreditRegistered(Integer.parseInt(values[4]));
				semPerformance.setCourseFailed(Integer.parseInt(values[5]));

				System.out.println("Extracted SemPerformance " + semPerformance);

				semPerformances.add(semPerformance);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logString.append("Extract," + semPerformances.size()
					+ ",Not Extracted,Data format is invalid - Further lines ignored\n");
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

		return semPerformances;
	}

	@SuppressWarnings("unchecked")
	public int transform(List<?> semPerformances, String instituteCode, String absoluteLogFileName)
			throws TransformException {
		int count = 0;
		try {
			for (SemPerformance semPerformance : (List<SemPerformance>) semPerformances) {
				semPerformance.setInstituteKey(instituteCode);
				semPerformance.setSplKey(instituteCode + '_' + semPerformance.getSplKey());
				semPerformance.setStudentKey(instituteCode + '_' + semPerformance.getStudentKey());
				System.out.println("Transformed SemPerformance " + semPerformance);
				count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw (new TransformException());
		}
		return count;
	}

	@SuppressWarnings("unchecked")
	public int load(List<?> semPerformances, String absoluteLogFileName) throws LoadException {
		int count = 0, processedLineCount = 0;
		StringBuffer logString = new StringBuffer();
		Connection con = DBConnection.getWriteConnection();
		SemPerformanceDAO semPerformanceDAO = new SemPerformanceDAO();

		try {
			for (SemPerformance semPerformance : (List<SemPerformance>) semPerformances) {
				try {
					++processedLineCount;
					count += semPerformanceDAO.addToDB(con, semPerformance);
					System.out.println("[UC] Loaded SemPerformance " + semPerformance);
				} catch (SQLException e) {
					logString.append("Load," + processedLineCount + ","
							+ semPerformance.getSplKey().replace(semPerformance.getInstituteKey() + "_", "") + ";"
							+ semPerformance.getStudentKey().replace(semPerformance.getInstituteKey() + "_", "") + ","
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
		SemPerformanceDAO semPerformanceDAO = new SemPerformanceDAO();

		try {
			semPerformanceDAO.addToHive(con, hadoopLocalFileName);
			System.out.println("[W] Warehoused SemPerformance file: " + hadoopLocalFileName);

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
