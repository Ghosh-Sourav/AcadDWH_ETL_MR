package in.ac.iitkgp.acaddwh.config;

public class ProjectInfo {
	private static String websiteName = "Academic Data Warehouse";
	private static String websiteTagLine = "Portal for managing academic data";

	private static String uploadDirPathWindows = "G:/AcadDWH/AcadDWH_Data/";
	private static String uploadDirPathLinux = "/home/mt1/15CS60R16/AcadDWH/AcadDWH_Data/";
	
	private static boolean constraintViolationReqd = false;

	public static String getWebsiteName() {
		return websiteName;
	}

	public static String getWebsiteTagLine() {
		return websiteTagLine;
	}

	public static String getUploadDirPath() {
		if (System.getProperty("os.name").contains("Windows")) {
			return uploadDirPathWindows;
		} else {
			return uploadDirPathLinux;
		}
	}

	public static boolean isConstraintViolationReqd() {
		return constraintViolationReqd;
	}

	

}
