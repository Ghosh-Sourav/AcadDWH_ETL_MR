package in.ac.iitkgp.acaddwh.service.etl.dim;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import in.ac.iitkgp.acaddwh.bean.dim.Specialisation;
import in.ac.iitkgp.acaddwh.dao.dim.SpecialisationDAO;
import in.ac.iitkgp.acaddwh.exception.ExtractException;
import in.ac.iitkgp.acaddwh.exception.LoadException;
import in.ac.iitkgp.acaddwh.exception.TransformException;
import in.ac.iitkgp.acaddwh.exception.WarehouseException;
import in.ac.iitkgp.acaddwh.service.ETLService;
import in.ac.iitkgp.acaddwh.util.DBConnection;
import in.ac.iitkgp.acaddwh.util.HiveConnection;
import in.ac.iitkgp.acaddwh.util.LogFile;

public class SpecialisationETL implements ETLService<Specialisation> {

	public List<?> extract(String filePath, String splitter, String absoluteLogFileName) throws ExtractException {
		List<Specialisation> specialisations = new ArrayList<Specialisation>();
		StringBuffer logString = new StringBuffer();
		BufferedReader br = null;
		String line = null;

		try {
			br = new BufferedReader(new FileReader(filePath));
			while ((line = br.readLine()) != null) {
				Specialisation specialisation = new Specialisation();
				String[] values = line.split(splitter);

				specialisation.setSplCode(values[0]);
				specialisation.setSplName(values[1]);
				specialisation.setDeptKey(values[2]);
				specialisation.setSplDegree(values[3]);
				specialisation.setSplLevel(values[4]);

				System.out.println("Extracted Specialisation " + specialisation);

				specialisations.add(specialisation);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logString.append("Extract," + specialisations.size()
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

		return specialisations;
	}

	@SuppressWarnings("unchecked")
	public int transform(List<?> specialisations, String instituteCode, String absoluteLogFileName)
			throws TransformException {
		int count = 0;
		try {
			for (Specialisation specialisation : (List<Specialisation>) specialisations) {
				specialisation.setSplKey(instituteCode + '_' + specialisation.getSplCode());
				specialisation.setDeptKey(instituteCode + '_' + specialisation.getDeptKey());
				System.out.println("Transformed Specialisation " + specialisation);
				count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw (new TransformException());
		}
		return count;
	}

	@SuppressWarnings("unchecked")
	public int load(List<?> specialisations, String absoluteLogFileName) throws LoadException {
		int count = 0, processedLineCount = 0;
		StringBuffer logString = new StringBuffer();

		Connection con = DBConnection.getWriteConnection();
		SpecialisationDAO specialisationDAO = new SpecialisationDAO();

		try {
			for (Specialisation specialisation : (List<Specialisation>) specialisations) {
				try {
					++processedLineCount;
					count += specialisationDAO.addToDB(con, specialisation);
					System.out.println("[UC] Loaded Specialisation " + specialisation);
				} catch (SQLException e) {
					logString.append("Load," + processedLineCount + "," + specialisation.getSplCode() + ","
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
		SpecialisationDAO specialisationDAO = new SpecialisationDAO();

		try {
			specialisationDAO.addToHive(con, hadoopLocalFileName);
			System.out.println("[W] Warehoused Specialisation file: " + hadoopLocalFileName);

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
