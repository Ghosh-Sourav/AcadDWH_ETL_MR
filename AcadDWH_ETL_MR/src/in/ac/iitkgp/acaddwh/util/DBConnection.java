package in.ac.iitkgp.acaddwh.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import in.ac.iitkgp.acaddwh.config.DBInfo;

public class DBConnection {

	public static Connection getWriteConnection() {
		Connection con = null;
		try {
			Class.forName(DBInfo.getDriverClass());
			con = DriverManager.getConnection(DBInfo.getUrl(), DBInfo.getUsername(), DBInfo.getPassword());
			con.setAutoCommit(false);
			con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return con;
	}

	public static Connection getReadConnection() {
		Connection con = null;
		try {
			Class.forName(DBInfo.getDriverClass());
			con = DriverManager.getConnection(DBInfo.getUrl(), DBInfo.getUsername(), DBInfo.getPassword());
			con.setAutoCommit(true);
			con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return con;
	}
	
	public static void closeConnection(Connection con) {
		if (con != null) {
			try {
				con.setAutoCommit(true);
				con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
	}

}