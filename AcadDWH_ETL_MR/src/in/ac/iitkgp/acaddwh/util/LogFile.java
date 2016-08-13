package in.ac.iitkgp.acaddwh.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;

public class LogFile {

	public static void writeToLogFile(String absoluteLogFileName, StringBuffer logString) {

		if (absoluteLogFileName != null) {

			BufferedWriter out = null;
			try {
				FileWriter fstream = new FileWriter(absoluteLogFileName, true);
				out = new BufferedWriter(fstream);
				out.write(new String(logString));
			} catch (IOException e) {
				System.err.println("Error: " + e.getMessage());
			} finally {
				if (out != null) {
					try {
						out.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

		}

	}

	public static String getErrorMsg(SQLException e) {
		String errorMsg = e.getMessage();

		if (errorMsg.contains("foreign key constraint")) {
			errorMsg = "At least one value is inconsistent with existing data in other dimensions";
		} else if (errorMsg.contains("duplicate key value violates unique constraint")) {
			errorMsg = "Key value is already present";
		} else {
			errorMsg = "Data is invalid";
			System.out.println(e.getMessage());
		}
		
		return errorMsg;
	}

}
