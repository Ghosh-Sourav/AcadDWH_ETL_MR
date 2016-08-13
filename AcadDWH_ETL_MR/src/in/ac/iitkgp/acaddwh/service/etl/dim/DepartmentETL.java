package in.ac.iitkgp.acaddwh.service.etl.dim;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import in.ac.iitkgp.acaddwh.bean.dim.Department;
import in.ac.iitkgp.acaddwh.dao.dim.DepartmentDAO;
import in.ac.iitkgp.acaddwh.exception.ExtractException;
import in.ac.iitkgp.acaddwh.exception.LoadException;
import in.ac.iitkgp.acaddwh.exception.TransformException;
import in.ac.iitkgp.acaddwh.exception.WarehouseException;
import in.ac.iitkgp.acaddwh.service.ETLService;
import in.ac.iitkgp.acaddwh.util.DBConnection;
import in.ac.iitkgp.acaddwh.util.HiveConnection;
import in.ac.iitkgp.acaddwh.util.LogFile;

public class DepartmentETL implements ETLService<Department> {

	public List<?> extract(String filePath, String splitter, String absoluteLogFileName) throws ExtractException {
		List<Department> departments = new ArrayList<Department>();
		StringBuffer logString = new StringBuffer();
		BufferedReader br = null;
		String line = null;

		try {
			br = new BufferedReader(new FileReader(filePath));
			while ((line = br.readLine()) != null) {
				Department department = new Department();
				String[] values = line.split(splitter);

				department.setDeptCode(values[0]);
				department.setDeptName(values[1]);
				department.setDeptDcsType(values[2]);

				System.out.println("Extracted Department " + department);

				departments.add(department);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logString.append("Extract," + departments.size()
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

		return departments;
	}

	@SuppressWarnings("unchecked")
	public int transform(List<?> departments, String instituteCode, String absoluteLogFileName)
			throws TransformException {
		int count = 0;
		try {
			for (Department department : (List<Department>) departments) {
				department.setDeptKey(instituteCode + '_' + department.getDeptCode());
				System.out.println("Transformed Department " + department);
				count++;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw (new TransformException());
		}
		return count;
	}

	@SuppressWarnings("unchecked")
	public int load(List<?> departments, String absoluteLogFileName) throws LoadException {
		int count = 0, processedLineCount = 0;
		StringBuffer logString = new StringBuffer();
		Connection con = DBConnection.getWriteConnection();
		DepartmentDAO departmentDAO = new DepartmentDAO();

		try {
			for (Department department : (List<Department>) departments) {
				try {
					++processedLineCount;
					count += departmentDAO.addToDB(con, department);
					System.out.println("[UC] Loaded Department " + department);
				} catch (SQLException e) {
					logString.append("Load," + processedLineCount + "," + department.getDeptCode() + ","
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
		DepartmentDAO departmentDAO = new DepartmentDAO();

		try {
			departmentDAO.addToHive(con, hadoopLocalFileName);
			System.out.println("[W] Warehoused Department file: " + hadoopLocalFileName);

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
