package in.ac.iitkgp.acaddwh;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class TestClass {

	public static void moveFile() {
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
			
			System.out.println("File move assumed to be complete!");
			
		}

	}

	
}
