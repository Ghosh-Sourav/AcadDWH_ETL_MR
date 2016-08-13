package in.ac.iitkgp.acaddwh.util;

import java.io.File;
import java.io.FileInputStream;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import in.ac.iitkgp.acaddwh.config.HadoopNodeInfo;
import in.ac.iitkgp.acaddwh.exception.WarehouseException;

public class SCP {

	/*
	 * Example: localFileName =
	 * "/home/sourav/AcadDWH/AcadDWH_Data/20160718035530444_IITKGP_dim_departments-hive.csv"
	 */
	public static String sendToHadoopNode(String localFileName) throws WarehouseException {

		String hadoopLocalFileName = null;
		try {
			JSch jsch = new JSch();

			Session session = jsch.getSession(HadoopNodeInfo.getUsername(), HadoopNodeInfo.getHadoopNodeIP(),
					HadoopNodeInfo.getHadoopNodePort());
			session.setPassword(HadoopNodeInfo.getPassword());
			session.setConfig("StrictHostKeyChecking", "no");
			session.connect();

			ChannelSftp channel = (ChannelSftp) session.openChannel("sftp");
			channel.connect();

			File localFile = new File(localFileName);

			channel.cd(HadoopNodeInfo.getPathInHadoopLocal());
			channel.put(new FileInputStream(localFile), localFile.getName());

			channel.disconnect();
			session.disconnect();

			hadoopLocalFileName = HadoopNodeInfo.getPathInHadoopLocal() + localFile.getName();
			System.out.println("Sent file " + localFileName + " over SCP; saved as " + hadoopLocalFileName);

		} catch (Exception e) {
			System.out.println("Failed to send file " + localFileName + " over SCP due to: " + e.getMessage());
			e.printStackTrace();
			throw (new WarehouseException());
		}

		return hadoopLocalFileName;
	}

}
