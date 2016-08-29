package in.ac.iitkgp.acaddwh.service.etl;

import java.util.HashMap;
import java.util.Map;

public class ETLHelper {
	private static Map<String, Long> etJobNameMaxTimeTakenMap = new HashMap<String, Long>();
	private static Map<String, Long> etJobNameTotalTimeTakenMap = new HashMap<String, Long>();

	public static void addMaxTimeTakenInfo(String jobName, long timeTaken) {
		if (etJobNameMaxTimeTakenMap.get(jobName) == null || etJobNameMaxTimeTakenMap.get(jobName) < timeTaken) {
			/* Value does not exist or is lower than current value */
			etJobNameMaxTimeTakenMap.put(jobName, timeTaken);
		}

		if (etJobNameTotalTimeTakenMap.get(jobName) == null) {
			addTotalTimeTakenInfo(jobName, timeTaken);
		} else {
			updateTotalTimeTakenInfo(jobName, timeTaken);
		}
	}

	public static Long getMaxTimeTakenInfo(String jobName) {
		return etJobNameMaxTimeTakenMap.get(jobName);
	}

	public static void removeMaxTimeTakenInfo(String jobName) {
		etJobNameMaxTimeTakenMap.remove(jobName);
		removeTotalTimeTakenInfo(jobName);
	}

	public static void addTotalTimeTakenInfo(String jobName, long timeTaken) {
		etJobNameTotalTimeTakenMap.put(jobName, timeTaken);
	}

	public static void updateTotalTimeTakenInfo(String jobName, long timeTaken) {
		long newTimeTaken = timeTaken + etJobNameTotalTimeTakenMap.get(jobName);
		etJobNameTotalTimeTakenMap.put(jobName, newTimeTaken);
	}

	public static Long getTotalTimeTakenInfo(String jobName) {
		return etJobNameTotalTimeTakenMap.get(jobName);
	}

	public static void removeTotalTimeTakenInfo(String jobName) {
		etJobNameTotalTimeTakenMap.remove(jobName);
	}

}
