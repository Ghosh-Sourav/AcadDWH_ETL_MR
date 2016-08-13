package in.ac.iitkgp.acaddwh.service.etl.dim;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import in.ac.iitkgp.acaddwh.bean.dim.Regtype;
import in.ac.iitkgp.acaddwh.dao.dim.RegtypeDAO;
import in.ac.iitkgp.acaddwh.exception.ExtractException;
import in.ac.iitkgp.acaddwh.exception.LoadException;
import in.ac.iitkgp.acaddwh.exception.TransformException;
import in.ac.iitkgp.acaddwh.exception.WarehouseException;
import in.ac.iitkgp.acaddwh.service.ETLService;
import in.ac.iitkgp.acaddwh.util.DBConnection;
import in.ac.iitkgp.acaddwh.util.HiveConnection;
import in.ac.iitkgp.acaddwh.util.LogFile;

public class RegtypeETL implements ETLService<Regtype> {

	public List<?> extract(String filePath, String splitter, String absoluteLogFileName) throws ExtractException {
		List<Regtype> regtypes = new ArrayList<Regtype>();
		StringBuffer logString = new StringBuffer();
		BufferedReader br = null;
		String line = null;

		try {
			br = new BufferedReader(new FileReader(filePath));
			while ((line = br.readLine()) != null) {
				Regtype regtype = new Regtype();
				String[] values = line.split(splitter);

				regtype.setRegtypeCode(values[0]);
				regtype.setRegtypeDesc(values[1]);

				System.out.println("Extracted Regtype " + regtype);

				regtypes.add(regtype);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logString.append(
					"Extract," + regtypes.size() + ",Not Extracted,Data format is invalid - Further lines ignored\n");
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

		return regtypes;
	}

	@SuppressWarnings("unchecked")
	public int transform(List<?> regtypes, String instituteCode, String absoluteLogFileName) throws TransformException {
		int count = 0;
		try {
			for (Regtype regtype : (List<Regtype>) regtypes) {
				regtype.setRegtypeKey(instituteCode + '_' + regtype.getRegtypeCode());
				System.out.println("Transformed Regtype " + regtype);
				count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw (new TransformException());
		}
		return count;
	}

	@SuppressWarnings("unchecked")
	public int load(List<?> regtypes, String absoluteLogFileName) throws LoadException {
		int count = 0, processedLineCount = 0;
		StringBuffer logString = new StringBuffer();

		Connection con = DBConnection.getWriteConnection();
		RegtypeDAO regtypeDAO = new RegtypeDAO();

		try {
			for (Regtype regtype : (List<Regtype>) regtypes) {
				try {
					++processedLineCount;
					count += regtypeDAO.addToDB(con, regtype);
					System.out.println("[UC] Loaded Regtype " + regtype);
				} catch (SQLException e) {
					logString.append("Load," + processedLineCount + "," + regtype.getRegtypeCode() + ","
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
		RegtypeDAO regtypeDAO = new RegtypeDAO();

		try {
			regtypeDAO.addToHive(con, hadoopLocalFileName);
			System.out.println("[W] Warehoused Regtype file: " + hadoopLocalFileName);

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
