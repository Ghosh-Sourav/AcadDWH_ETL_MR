package in.ac.iitkgp.acaddwh.service.etl.fact;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import in.ac.iitkgp.acaddwh.bean.fact.SplPerformance;
import in.ac.iitkgp.acaddwh.dao.fact.SplPerformanceDAO;
import in.ac.iitkgp.acaddwh.exception.ExtractException;
import in.ac.iitkgp.acaddwh.exception.LoadException;
import in.ac.iitkgp.acaddwh.exception.TransformException;
import in.ac.iitkgp.acaddwh.exception.WarehouseException;
import in.ac.iitkgp.acaddwh.service.ETLService;
import in.ac.iitkgp.acaddwh.util.DBConnection;
import in.ac.iitkgp.acaddwh.util.HiveConnection;
import in.ac.iitkgp.acaddwh.util.LogFile;

public class SplPerformanceETL implements ETLService<SplPerformance> {

	public List<?> extract(String filePath, String splitter, String absoluteLogFileName) throws ExtractException {
		List<SplPerformance> splPerformances = new ArrayList<SplPerformance>();
		StringBuffer logString = new StringBuffer();
		BufferedReader br = null;
		String line = null;

		try {
			br = new BufferedReader(new FileReader(filePath));
			while ((line = br.readLine()) != null) {
				SplPerformance splPerformance = new SplPerformance();
				String[] values = line.split(splitter);

				splPerformance.setSplKey(values[0]);
				splPerformance.setTimeKey(values[1]);
				splPerformance.setAdmStuCnt(Integer.parseInt(values[2]));
				splPerformance.setOnrollStuCnt(Integer.parseInt(values[3]));
				splPerformance.setGradStuCnt(Integer.parseInt(values[4]));
				splPerformance.setDropoutStuCnt(Integer.parseInt(values[5]));
				splPerformance.setPercentPlaced(Integer.parseInt(values[6]));
				splPerformance.setAvgSalary(Float.parseFloat(values[7]));

				System.out.println("Extracted SplPerformance " + splPerformance);

				splPerformances.add(splPerformance);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logString.append("Extract," + splPerformances.size()
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

		return splPerformances;
	}

	@SuppressWarnings("unchecked")
	public int transform(List<?> splPerformances, String instituteCode, String absoluteLogFileName)
			throws TransformException {
		int count = 0;
		try {
			for (SplPerformance splPerformance : (List<SplPerformance>) splPerformances) {
				splPerformance.setInstituteKey(instituteCode);
				splPerformance.setSplKey(instituteCode + '_' + splPerformance.getSplKey());
				splPerformance.setTimeKey(instituteCode + '_' + splPerformance.getTimeKey());
				System.out.println("Transformed SplPerformance " + splPerformance);
				count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw (new TransformException());
		}
		return count;
	}

	@SuppressWarnings("unchecked")
	public int load(List<?> splPerformances, String absoluteLogFileName) throws LoadException {
		int count = 0, processedLineCount = 0;
		StringBuffer logString = new StringBuffer();
		Connection con = DBConnection.getWriteConnection();
		SplPerformanceDAO splPerformanceDAO = new SplPerformanceDAO();

		try {
			for (SplPerformance splPerformance : (List<SplPerformance>) splPerformances) {
				try {
					++processedLineCount;
					count += splPerformanceDAO.addToDB(con, splPerformance);
					System.out.println("[UC] Loaded SplPerformance " + splPerformance);
				} catch (SQLException e) {
					logString.append("Load," + processedLineCount + ","
							+ splPerformance.getSplKey().replace(splPerformance.getInstituteKey() + "_", "") + ";"
							+ splPerformance.getTimeKey().replace(splPerformance.getInstituteKey() + "_", "") + ","
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
		SplPerformanceDAO splPerformanceDAO = new SplPerformanceDAO();

		try {
			splPerformanceDAO.addToHive(con, hadoopLocalFileName);
			System.out.println("[W] Warehoused SplPerformance file: " + hadoopLocalFileName);

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
