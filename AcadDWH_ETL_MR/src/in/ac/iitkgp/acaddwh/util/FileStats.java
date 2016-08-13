package in.ac.iitkgp.acaddwh.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import in.ac.iitkgp.acaddwh.config.ProjectInfo;

public class FileStats {
	public static long getLineCount(String fileName) {
		String absoluteFileName = ProjectInfo.getUploadDirPath() + fileName;

		InputStream is = null;
		try {
			is = new BufferedInputStream(new FileInputStream(absoluteFileName));
			byte[] c = new byte[1024];
			long count = 0;
			int readChars = 0;
			boolean endsWithoutNewLine = false;
			while ((readChars = is.read(c)) != -1) {
				for (int i = 0; i < readChars; ++i) {
					if (c[i] == '\n')
						++count;
				}
				endsWithoutNewLine = (c[readChars - 1] != '\n');
			}
			if (endsWithoutNewLine) {
				++count;
			}
			return count;
		} catch (Exception e) {
			return -1;
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
				}
			}
		}
	}

	public static long getSizeInBytes(String fileName) {
		String absoluteFileName = ProjectInfo.getUploadDirPath() + fileName;
		long size;
		try {
			size = (new File(absoluteFileName).length());
		} catch (Exception e) {
			size = -1;
		}
		return size;
	}

}
