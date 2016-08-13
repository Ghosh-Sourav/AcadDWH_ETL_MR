package in.ac.iitkgp.acaddwh.config;

public class HiveInfo {
	private static String driverClass = 
			"org.apache.hive.jdbc.HiveDriver";		// For Hive 2
			//"org.apache.hadoop.hive.jdbc.HiveDriver";	// For Hive 1
	private static String protocol = "jdbc:hive2:";
	private static String hostIP = 
			//"7.224.118.49";		// For authenticated remote access
			"10.5.30.101";	// For internal access
	private static long hostPort = 10000;
	private static String databaseName = "acaddwh";
	private static String username = "15CS60R16";
	private static String password = "";

	private static String url = protocol + "//" + hostIP + ":" + hostPort + "/" + databaseName;
	
	public static String getDriverClass() {
		return driverClass;
	}

	public static String getProtocol() {
		return protocol;
	}

	protected static String getHostIP() {
		return hostIP;
	}

	protected static long getHostPort() {
		return hostPort;
	}

	protected static String getDatabaseName() {
		return databaseName;
	}

	public static String getUsername() {
		return username;
	}

	public static String getPassword() {
		return password;
	}

	public static String getUrl() {
		return url;
	}

}
