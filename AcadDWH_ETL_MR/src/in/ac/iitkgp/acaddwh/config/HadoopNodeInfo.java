package in.ac.iitkgp.acaddwh.config;

import in.ac.iitkgp.acaddwh.util.FileStats;

public class HadoopNodeInfo {

	private static String hadoopNodeIP =
	// "7.224.118.49"; // For authenticated remote access
	"10.5.30.101"; // For internal access
	private static int hadoopNodePort = 22;

	private static String username = "15CS60R16";
	private static String password = "";

	private static String pathInHadoopLocal = "/home/mtech/15CS60R16/AcadDWH/AcadDWH_Data/";
	private static String pathInHdfs = "/user/15CS60R16/AcadDWH/AcadDWH_Data/";

	private enum MapCount {
		ONE_MAPPER, PROPORTIONAL_TO_FILESIZE
	}

	private static MapCount no_of_mappersRequired = MapCount.ONE_MAPPER;

	private static long splitSize = 2 * 1024 * 1024; // in bytes
	private static long dfsBlockSize = 1 * 1024 * 1024; // in bytes

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

	public static String getPathInHdfs() {
		return pathInHdfs;
	}

	public static long getSplitSize() {
		return splitSize;
	}

	public static long getSplitSize(String shortFileName) {
		if (no_of_mappersRequired == MapCount.ONE_MAPPER) {
			return FileStats.getSizeInBytes(shortFileName);
		} else if (no_of_mappersRequired == MapCount.PROPORTIONAL_TO_FILESIZE) {
			return splitSize;
		} else {
			return -1;
		}

	}

	public static long getDfsBlockSize() {
		return dfsBlockSize;
	}

}
