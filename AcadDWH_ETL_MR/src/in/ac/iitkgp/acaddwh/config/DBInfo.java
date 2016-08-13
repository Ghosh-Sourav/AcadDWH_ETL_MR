package in.ac.iitkgp.acaddwh.config;

public class DBInfo {
	private static String driverClass = "org.postgresql.Driver";
	private static String protocol = "jdbc:postgresql:";
	private static String hostIP = "10.5.23.218";
	private static long hostPort = 5432;
	private static String databaseName = "dwh";
	private static String username = "sourav";
	private static String password = "$herlock";

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
