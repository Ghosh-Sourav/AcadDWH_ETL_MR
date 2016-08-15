package in.ac.iitkgp.acaddwh.util;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import in.ac.iitkgp.acaddwh.config.HadoopNodeInfo;
import in.ac.iitkgp.acaddwh.config.NameNodeInfo;

public class HdfsFileTransfer {

	/*
	 * Example:
	 * 
	 * localfsFilePath = "/home/mtech/15CS60R16/AcadDWH/AcadDWH_Data/x.csv";
	 * 
	 * hdfsFilePath = "/user/15CS60R16/AcadDWH/AcadDWH_Data/x.csv";
	 * 
	 */
	public static void copyFileToHdfs(String localfsFilePath, String hdfsFilePath) throws IOException {

		try {
			System.out.println("File at " + localfsFilePath + " is to be copied to " + hdfsFilePath + " ...");

			Configuration config = new Configuration();
			config.set("fs.defaultFS", NameNodeInfo.getUrl());

			FileSystem hdfs = FileSystem.get(config);

			Path srcPath = new Path(localfsFilePath);
			Path dstPath = new Path(hdfsFilePath);

			hdfs.copyFromLocalFile(srcPath, dstPath);

		} catch (IOException e) {
			e.printStackTrace();
			throw (e);

		} finally {
			System.out.println("File at " + localfsFilePath + " copied to " + hdfsFilePath);

		}

	}

	/*
	 * Generates hdfsFilePath, if not provided
	 */
	public static void copyFileToHdfs(String localfsFilePath) throws IOException {

		File localFile = new File(localfsFilePath);

		String hdfsFilePath = HadoopNodeInfo.getPathInHdfs() + localFile.getName();

		copyFileToHdfs(localfsFilePath, hdfsFilePath);

	}
}
