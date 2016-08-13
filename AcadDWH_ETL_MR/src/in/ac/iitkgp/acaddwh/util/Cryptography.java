package in.ac.iitkgp.acaddwh.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Cryptography {
	public static String encrypt(String plaintext) {
		String ciphertext = null;

		MessageDigest messageDigest;
		try {
			messageDigest = MessageDigest.getInstance("SHA-256");
			byte[] result = messageDigest.digest(plaintext.getBytes());
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < result.length; i++) {
				stringBuffer.append(Integer.toString(
						(result[i] & 0xff) + 0x100, 16).substring(1));
			}
			ciphertext = new String(stringBuffer);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		return ciphertext;
	}

}
