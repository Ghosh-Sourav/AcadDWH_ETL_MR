package in.ac.iitkgp.acaddwh.service;

import in.ac.iitkgp.acaddwh.exception.*;

public interface ETLService<T> {

	// public List<?> extract(String filePath, String splitter, String
	// absoluteLogFileName) throws ExtractException;
	//
	// public int transform(List<?> items, String uniqueKeyFragment, String
	// absoluteLogFileName) throws TransformException;

	public boolean extractAndTransform(String shortFileName, String instituteCode, String absoluteLogFileName)
			throws ExtractAndTransformException;

	public void load(String hdfsFilePath, String absoluteLogFileName) throws LoadException;

}
