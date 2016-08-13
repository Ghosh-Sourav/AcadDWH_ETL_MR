package in.ac.iitkgp.acaddwh.service.etl.fact;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import in.ac.iitkgp.acaddwh.bean.fact.StuLearning;
import in.ac.iitkgp.acaddwh.dao.fact.StuLearningDAO;
import in.ac.iitkgp.acaddwh.exception.ExtractException;
import in.ac.iitkgp.acaddwh.exception.LoadException;
import in.ac.iitkgp.acaddwh.exception.TransformException;
import in.ac.iitkgp.acaddwh.exception.WarehouseException;
import in.ac.iitkgp.acaddwh.service.ETLService;
import in.ac.iitkgp.acaddwh.util.DBConnection;
import in.ac.iitkgp.acaddwh.util.HiveConnection;
import in.ac.iitkgp.acaddwh.util.LogFile;

public class StuLearningETL implements ETLService<StuLearning> {

	public List<?> extract(String filePath, String splitter, String absoluteLogFileName) throws ExtractException {
		List<StuLearning> stuLearnings = new ArrayList<StuLearning>();
		StringBuffer logString = new StringBuffer();
		BufferedReader br = null;
		String line = null;

		try {
			br = new BufferedReader(new FileReader(filePath));
			long lineNumber = 0;
			while ((line = br.readLine()) != null) {
				StuLearning stuLearning = new StuLearning();
				String[] values = line.split(splitter);

				stuLearning.setCourseKey(values[0]);
				stuLearning.setTimeKey(values[1]);
				stuLearning.setStudentKey(values[2]);
				stuLearning.setRegtypeKey(values[3]);
				stuLearning.setGrade(values[4]);
				stuLearning.setNumGrade(Integer.parseInt(values[5]));
				stuLearning.setImprFactor(Float.parseFloat(values[6]));
				stuLearning.setPrFnImpr(Float.parseFloat(values[7]));
				stuLearning.setPrPsImpr(Float.parseFloat(values[8]));
				stuLearning.setPercentAttended(Float.parseFloat(values[9]));

				++lineNumber;
				System.out.println("Extracted StuLearning " + lineNumber + stuLearning);

				stuLearnings.add(stuLearning);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logString.append("Extract," + stuLearnings.size()
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

		return stuLearnings;
	}

	@SuppressWarnings("unchecked")
	public int transform(List<?> stuLearnings, String instituteCode, String absoluteLogFileName)
			throws TransformException {
		int count = 0;
		try {
			for (StuLearning stuLearning : (List<StuLearning>) stuLearnings) {
				stuLearning.setInstituteKey(instituteCode);
				stuLearning.setCourseKey(instituteCode + '_' + stuLearning.getCourseKey());
				stuLearning.setTimeKey(instituteCode + '_' + stuLearning.getTimeKey());
				stuLearning.setStudentKey(instituteCode + '_' + stuLearning.getStudentKey());
				stuLearning.setRegtypeKey(instituteCode + '_' + stuLearning.getRegtypeKey());
				System.out.println("Transformed StuLearning " + stuLearning);
				count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw (new TransformException());
		}
		return count;
	}

	@SuppressWarnings("unchecked")
	public int load(List<?> stuLearnings, String absoluteLogFileName) throws LoadException {
		int count = 0, processedLineCount = 0;
		StringBuffer logString = new StringBuffer();
		Connection con = DBConnection.getWriteConnection();
		StuLearningDAO stuLearningDAO = new StuLearningDAO();

		try {
			for (StuLearning stuLearning : (List<StuLearning>) stuLearnings) {
				try {
					++processedLineCount;
					count += stuLearningDAO.addToDB(con, stuLearning);
					System.out.println("[UC] Loaded StuLearning " + stuLearning);
				} catch (SQLException e) {
					logString.append("Load," + processedLineCount + ","
							+ stuLearning.getCourseKey().replace(stuLearning.getInstituteKey() + "_", "") + ";"
							+ stuLearning.getTimeKey().replace(stuLearning.getInstituteKey() + "_", "") + ";"
							+ stuLearning.getStudentKey().replace(stuLearning.getInstituteKey() + "_", "") + ";"
							+ stuLearning.getRegtypeKey().replace(stuLearning.getInstituteKey() + "_", "") + ","
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
		StuLearningDAO stuLearningDAO = new StuLearningDAO();

		try {
			stuLearningDAO.addToHive(con, hadoopLocalFileName);
			System.out.println("[W] Warehoused StuLearning file: " + hadoopLocalFileName);

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
