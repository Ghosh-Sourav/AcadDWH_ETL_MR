package in.ac.iitkgp.acaddwh.dao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import in.ac.iitkgp.acaddwh.bean.Test;

public class TestDAO {

	public void moveFile() {
		String localfsFilePath = "/home/sourav/AcadDWH/test_table_data.csv";
		String hdfsFilePath = "/user/15CS60R16/AcadDWH/test_table_data.csv";

		try {
			Configuration config = new Configuration();
			config.set("fs.defaultFS", "hdfs://10.5.30.101:18020");
			FileSystem hdfs = FileSystem.get(config);
			Path srcPath = new Path(localfsFilePath);
			Path dstPath = new Path(hdfsFilePath);
			hdfs.copyFromLocalFile(srcPath, dstPath);
		} catch (IOException e) {
			System.out.println("Irrelevant IOException occurred in TestDAO... ignored! "+e.getMessage());
		}
		finally {
			System.out.println("Initiated move... Waiting...");
			try {
				wait(30);
			} catch (InterruptedException e) {
			}
			System.out.println("File move assumed to be complete!");
			
		}

	}

	public int saveDim(Connection con, Test test) throws SQLException {
		int returnValue = 0;
		PreparedStatement ps = null;

		try {
			String hadoopMachineLocalFilePath = "/home/mtech/15CS60R16/AcadDWH/20160718031631256_IITKGP_dim_departments-hive.csv";
			//String hdfsFilePath = "/user/15CS60R16/AcadDWH/test_table_data.csv";

			// ps = con.prepareStatement("insert into table test_table select ?,
			// ? from dummy");
			// ps.setInt(1, test.getRoll());
			// ps.setString(2, test.getName());

			ps = con.prepareStatement("LOAD DATA LOCAL INPATH ? INTO TABLE acaddwh.dim_departments");

			ps.setString(1, hadoopMachineLocalFilePath);

			System.out.println("Name = " + test.getName() + ", Roll = " + test.getRoll());

			returnValue = ps.executeUpdate();

		} finally {
			if (ps != null) {
				ps.close();
			}
		}
		return returnValue;
	}

	public void printItems(Connection con) throws SQLException {
		ResultSet rs = null;
		PreparedStatement ps = null;

		try {
			ps = con.prepareStatement("select * from acaddwh.test_table where roll<20");

			rs = ps.executeQuery();
			while (rs.next()) {

				System.out.println("Column 1 = " + rs.getString(1));
				System.out.println("Column 2 = " + rs.getString(2));

			}

		} finally {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
		}

	}
}
