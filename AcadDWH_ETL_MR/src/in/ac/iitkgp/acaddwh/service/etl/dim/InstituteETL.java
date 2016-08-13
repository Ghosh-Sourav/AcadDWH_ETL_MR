package in.ac.iitkgp.acaddwh.service.etl.dim;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import in.ac.iitkgp.acaddwh.bean.dim.Institute;
import in.ac.iitkgp.acaddwh.dao.dim.InstituteDAO;
import in.ac.iitkgp.acaddwh.exception.ExtractException;
import in.ac.iitkgp.acaddwh.exception.LoadException;
import in.ac.iitkgp.acaddwh.exception.TransformException;
import in.ac.iitkgp.acaddwh.exception.WarehouseException;
import in.ac.iitkgp.acaddwh.service.ETLService;
import in.ac.iitkgp.acaddwh.util.Cryptography;
import in.ac.iitkgp.acaddwh.util.DBConnection;
import in.ac.iitkgp.acaddwh.util.HiveConnection;
import in.ac.iitkgp.acaddwh.util.LogFile;

public class InstituteETL implements ETLService<Institute> {

	public List<?> extract(String filePath, String splitter, String absoluteLogFileName) throws ExtractException {
		List<Institute> institutes = new ArrayList<Institute>();
		StringBuffer logString = new StringBuffer();
		BufferedReader br = null;
		String line = null;

		try {
			br = new BufferedReader(new FileReader(filePath));
			while ((line = br.readLine()) != null) {
				Institute institute = new Institute();
				String[] values = line.split(splitter);

				institute.setInstituteKey(values[0]);
				institute.setInstituteName(values[1]);

				System.out.println("Extracted Institute " + institute);

				institutes.add(institute);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logString.append(
					"Extract," + institutes.size() + ",Not Extracted,Data format is invalid - Further lines ignored\n");
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

		return institutes;
	}

	@SuppressWarnings("unchecked")
	public int transform(List<?> institutes, String dummyKey, String absoluteLogFileName) throws TransformException {
		int count = 0;
		try {
			for (Institute institute : (List<Institute>) institutes) {
				institute.setInstituteKey(institute.getInstituteKey());
				institute.setInstitutePassword(
						Cryptography.encrypt(institute.getInstituteKey() + institute.getInstitutePassword()));
				System.out.println("Transformed Institute " + institute);
				count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw (new TransformException());
		}
		return count;
	}

	@SuppressWarnings("unchecked")
	public int load(List<?> institutes, String absoluteLogFileName) throws LoadException {
		int count = 0, processedLineCount = 0;
		StringBuffer logString = new StringBuffer();

		Connection con = DBConnection.getWriteConnection();
		InstituteDAO instituteDAO = new InstituteDAO();

		try {
			for (Institute institute : (List<Institute>) institutes) {
				try {
					++processedLineCount;
					count += instituteDAO.addToDB(con, institute);
					System.out.println("[UC] Loaded Institute " + institute);
				} catch (SQLException e) {
					logString.append("Load," + processedLineCount + "," + institute.getInstituteKey() + ","
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
		InstituteDAO instituteDAO = new InstituteDAO();

		try {
			instituteDAO.addToHive(con, hadoopLocalFileName);
			System.out.println("[W] Warehoused Institute file: " + hadoopLocalFileName);

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
