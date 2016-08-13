package in.ac.iitkgp.acaddwh.service.etl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import in.ac.iitkgp.acaddwh.bean.Test;
import in.ac.iitkgp.acaddwh.config.HiveInfo;
import in.ac.iitkgp.acaddwh.dao.TestDAO;
import in.ac.iitkgp.acaddwh.exception.WarehouseException;
import in.ac.iitkgp.acaddwh.util.HiveConnection;
import in.ac.iitkgp.acaddwh.util.LogFile;

public class TestHiveLoad {

	@SuppressWarnings("unchecked")
	public int load(List<?> tests, String absoluteLogFileName) throws WarehouseException {
		int count = 0, processedLineCount = 0;
		StringBuffer logString = new StringBuffer();

		Connection con = null;

		TestDAO testDAO = new TestDAO();

		try {

			con = HiveConnection.getSaveConnection();
			System.out.println("Connection obtained!");

			testDAO.printItems(con);

			for (Test test : (List<Test>) tests) {
				try {
					++processedLineCount;
					//testDAO.moveFile();
					count += testDAO.saveDim(con, test);
					System.out.println("[W] Warehoused Test " + test);
				} catch (SQLException e) {
					logString.append("Warehouse," + processedLineCount + "," + test.getRoll() + ","
							+ LogFile.getErrorMsg(e) + ": " + e.getMessage() + "\n");
				}
			}
			if (logString.length() != 0) {
				throw (new WarehouseException());
			}
			System.out.println("Submitted for warehousing!");
		} catch (Exception e) {
			System.out.println("Warehousing failed!");
			System.out.println(logString);
			System.out.println("garbage at "+absoluteLogFileName);
			//LogFile.writeToLogFile(absoluteLogFileName, logString);
			count = 0;
			e.printStackTrace();
			throw (new WarehouseException());
		} finally {
			HiveConnection.closeConnection(con);
		}

		return count;
	}

	public static void main(String[] args) {
		List<Test> tests = new ArrayList<Test>();

		Test test = new Test();
		test.setName("Sourav Ghosh 1234");
		test.setRoll(16);

		tests.add(test);

		TestHiveLoad testHiveLoad = new TestHiveLoad();
		try {
			System.out.println("Starting...");
			System.out.println(HiveInfo.getUrl());
			testHiveLoad.load(tests, "");
			System.out.println("Finished with success!");
		} catch (WarehouseException e) {
			e.printStackTrace();
		}
		System.out.println();

	}

}
