package in.ac.iitkgp.acaddwh.service.etl.dim;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import in.ac.iitkgp.acaddwh.bean.dim.EvalArea;
import in.ac.iitkgp.acaddwh.dao.dim.EvalAreaDAO;
import in.ac.iitkgp.acaddwh.exception.ExtractException;
import in.ac.iitkgp.acaddwh.exception.LoadException;
import in.ac.iitkgp.acaddwh.exception.TransformException;
import in.ac.iitkgp.acaddwh.exception.WarehouseException;
import in.ac.iitkgp.acaddwh.service.ETLService;
import in.ac.iitkgp.acaddwh.util.DBConnection;
import in.ac.iitkgp.acaddwh.util.HiveConnection;
import in.ac.iitkgp.acaddwh.util.LogFile;

public class EvalAreaETL implements ETLService<EvalArea> {

	public List<?> extract(String filePath, String splitter, String absoluteLogFileName) throws ExtractException {
		List<EvalArea> evalAreas = new ArrayList<EvalArea>();
		StringBuffer logString = new StringBuffer();
		BufferedReader br = null;
		String line = null;

		try {
			br = new BufferedReader(new FileReader(filePath));
			while ((line = br.readLine()) != null) {
				EvalArea evalArea = new EvalArea();
				String[] values = line.split(splitter);

				evalArea.setEvalAreaCode(values[0]);
				evalArea.setEvalArea(values[1]);

				System.out.println("Extracted EvalArea " + evalArea);

				evalAreas.add(evalArea);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logString.append(
					"Extract," + evalAreas.size() + ",Not Extracted,Data format is invalid - Further lines ignored\n");
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

		return evalAreas;
	}

	@SuppressWarnings("unchecked")
	public int transform(List<?> evalAreas, String instituteCode, String absoluteLogFileName)
			throws TransformException {
		int count = 0;
		try {
			for (EvalArea evalArea : (List<EvalArea>) evalAreas) {
				evalArea.setEvalAreaKey(instituteCode + '_' + evalArea.getEvalAreaCode());
				System.out.println("Transformed EvalArea " + evalArea);
				count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw (new TransformException());
		}
		return count;
	}

	@SuppressWarnings("unchecked")
	public int load(List<?> evalAreas, String absoluteLogFileName) throws LoadException {
		int count = 0, processedLineCount = 0;
		StringBuffer logString = new StringBuffer();

		Connection con = DBConnection.getWriteConnection();
		EvalAreaDAO evalAreaDAO = new EvalAreaDAO();

		try {
			for (EvalArea evalArea : (List<EvalArea>) evalAreas) {
				try {
					++processedLineCount;
					count += evalAreaDAO.addToDB(con, evalArea);
					System.out.println("[UC] Loaded EvalArea " + evalArea);
				} catch (SQLException e) {
					logString.append("Load," + processedLineCount + "," + evalArea.getEvalAreaCode() + ","
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
		EvalAreaDAO evalAreaDAO = new EvalAreaDAO();

		try {
			evalAreaDAO.addToHive(con, hadoopLocalFileName);
			System.out.println("[W] Warehoused EvalArea file: " + hadoopLocalFileName);

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
