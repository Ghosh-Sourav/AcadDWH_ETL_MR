package in.ac.iitkgp.acaddwh;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import in.ac.iitkgp.acaddwh.config.HadoopNodeInfo;
import in.ac.iitkgp.acaddwh.config.NameNodeInfo;

public class TestHDFSList {
	public static List<String> getFilePaths() throws Exception {
		List<String> filePaths = new ArrayList<String>();
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] fileStatus = fs.listStatus(new Path(NameNodeInfo.getUrl() + HadoopNodeInfo.getPathInHdfs()
					+ "outputDir_20160815213022114_INSA_dim_departments_csv"));

			for (FileStatus eachFileStatus : fileStatus) {
				if (eachFileStatus.isFile() && eachFileStatus.getPath().getName().contains("part-r-")) {
					filePaths.add(eachFileStatus.getPath().toString());
		        }
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return filePaths;
	}
}
