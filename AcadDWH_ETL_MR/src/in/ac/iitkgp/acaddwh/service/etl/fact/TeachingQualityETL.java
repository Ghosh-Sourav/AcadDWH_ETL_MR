package in.ac.iitkgp.acaddwh.service.etl.fact;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import in.ac.iitkgp.acaddwh.bean.fact.TeachingQuality;
import in.ac.iitkgp.acaddwh.dao.fact.TeachingQualityDAO;
import in.ac.iitkgp.acaddwh.exception.ExtractException;
import in.ac.iitkgp.acaddwh.exception.LoadException;
import in.ac.iitkgp.acaddwh.exception.TransformException;
import in.ac.iitkgp.acaddwh.exception.WarehouseException;
import in.ac.iitkgp.acaddwh.service.ETLService;
import in.ac.iitkgp.acaddwh.util.DBConnection;
import in.ac.iitkgp.acaddwh.util.HiveConnection;
import in.ac.iitkgp.acaddwh.util.LogFile;

public class TeachingQualityETL implements ETLService<TeachingQuality> {

	public List<?> extract(String filePath, String splitter, String absoluteLogFileName) throws ExtractException {
		List<TeachingQuality> teachingQualitys = new ArrayList<TeachingQuality>();
		StringBuffer logString = new StringBuffer();
		BufferedReader br = null;
		String line = null;

		try {
			br = new BufferedReader(new FileReader(filePath));
			long lineNumber = 0;
			while ((line = br.readLine()) != null) {
				TeachingQuality teachingQuality = new TeachingQuality();
				String[] values = line.split(splitter);

				teachingQuality.setCourseKey(values[0]);
				teachingQuality.setTimeKey(values[1]);
				teachingQuality.setTeacherKey(values[2]);
				teachingQuality.setEvalAreaKey(values[3]);
				teachingQuality.setNoOfEvaluation(Integer.parseInt(values[4]));
				teachingQuality.setAvgTeachingQuality(Float.parseFloat(values[5]));

				++lineNumber;
				System.out.println("Extracted TeachingQuality " + lineNumber + teachingQuality);

				teachingQualitys.add(teachingQuality);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logString.append("Extract," + teachingQualitys.size()
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

		return teachingQualitys;
	}

	@SuppressWarnings("unchecked")
	public int transform(List<?> teachingQualitys, String instituteCode, String absoluteLogFileName)
			throws TransformException {
		int count = 0;
		try {
			for (TeachingQuality teachingQuality : (List<TeachingQuality>) teachingQualitys) {
				teachingQuality.setInstituteKey(instituteCode);
				teachingQuality.setCourseKey(instituteCode + '_' + teachingQuality.getCourseKey());
				teachingQuality.setTimeKey(instituteCode + '_' + teachingQuality.getTimeKey());
				teachingQuality.setTeacherKey(instituteCode + '_' + teachingQuality.getTeacherKey());
				teachingQuality.setEvalAreaKey(instituteCode + '_' + teachingQuality.getEvalAreaKey());
				System.out.println("Transformed TeachingQuality " + teachingQuality);
				count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw (new TransformException());
		}
		return count;
	}

	@SuppressWarnings("unchecked")
	public int load(List<?> teachingQualitys, String absoluteLogFileName) throws LoadException {
		int count = 0, processedLineCount = 0;
		StringBuffer logString = new StringBuffer();
		Connection con = DBConnection.getWriteConnection();
		TeachingQualityDAO teachingQualityDAO = new TeachingQualityDAO();

		try {
			for (TeachingQuality teachingQuality : (List<TeachingQuality>) teachingQualitys) {
				try {
					++processedLineCount;
					count += teachingQualityDAO.addToDB(con, teachingQuality);
					System.out.println("[UC] Loaded TeachingQuality " + teachingQuality);
				} catch (SQLException e) {
					logString.append("Load," + processedLineCount + ","
							+ teachingQuality.getCourseKey().replace(teachingQuality.getInstituteKey() + "_", "") + ";"
							+ teachingQuality.getTimeKey().replace(teachingQuality.getInstituteKey() + "_", "") + ";"
							+ teachingQuality.getTeacherKey().replace(teachingQuality.getInstituteKey() + "_", "") + ";"
							+ teachingQuality.getEvalAreaKey().replace(teachingQuality.getInstituteKey() + "_", "")
							+ "," + LogFile.getErrorMsg(e) + "\n");
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
		TeachingQualityDAO teachingQualityDAO = new TeachingQualityDAO();

		try {
			teachingQualityDAO.addToHive(con, hadoopLocalFileName);
			System.out.println("[W] Warehoused TeachingQuality file: " + hadoopLocalFileName);

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
