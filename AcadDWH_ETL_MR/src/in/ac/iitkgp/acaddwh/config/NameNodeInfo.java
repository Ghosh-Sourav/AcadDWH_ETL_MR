package in.ac.iitkgp.acaddwh.config;

public class NameNodeInfo {

	private static String protocol = "hdfs:";
	private static String hostName = "cse-hadoop-109";
	private static int hostPort = 8020;

	private static String url = protocol + "//" + hostName + ":" + hostPort;

	public static String getUrl() {
		return url;
	}

}
