package in.ac.iitkgp.acaddwh.util;

import java.text.SimpleDateFormat;
import java.util.Date;

import in.ac.iitkgp.acaddwh.exception.KeyGenerationException;

public class KeyRepository {

	public synchronized static String getKey() throws KeyGenerationException {
		String key = null;

		try {
			Thread.sleep(317);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw (new KeyGenerationException());
		}

		Date date = new Date();
		SimpleDateFormat kf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		key = kf.format(date);

		return key;
	}

}
