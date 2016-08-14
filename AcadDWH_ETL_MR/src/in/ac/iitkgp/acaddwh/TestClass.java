package in.ac.iitkgp.acaddwh;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestClass {

	public static void moveFile() {
		String localfsFilePath = "/home/mtech/15CS60R16/x.csv";
		String hdfsFilePath = "/user/15CS60R16/TestDir/x.csv";

		try {
			Configuration config = new Configuration();
			config.set("fs.defaultFS", "hdfs://cse-hadoop-109:8020");
			FileSystem hdfs = FileSystem.get(config);
			Path srcPath = new Path(localfsFilePath);
			Path dstPath = new Path(hdfsFilePath);
			hdfs.copyFromLocalFile(srcPath, dstPath);
			System.out.println("Initiated move...");

		} catch (IOException e) {
			e.printStackTrace();

		} finally {
			System.out.println("File move processed!");

		}

	}

}
