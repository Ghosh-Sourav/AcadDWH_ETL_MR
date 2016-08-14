package in.ac.iitkgp.acaddwh.config;

public class HadoopNodeInfo {

	private static String hadoopNodeIP =
	// "7.224.118.49"; // For authenticated remote access
	"10.5.30.101"; // For internal access
	private static int hadoopNodePort = 22;

	private static String username = "15CS60R16";
	private static String password = "";

	private static String pathInHadoopLocal = "/home/mtech/15CS60R16/AcadDWH_Data/";

	public static String getHadoopNodeIP() {
		return hadoopNodeIP;
	}

	public static int getHadoopNodePort() {
		return hadoopNodePort;
	}

	public static String getUsername() {
		return username;
	}

	public static String getPassword() {
		return password;
	}

	public static String getPathInHadoopLocal() {
		return pathInHadoopLocal;
	}

}
