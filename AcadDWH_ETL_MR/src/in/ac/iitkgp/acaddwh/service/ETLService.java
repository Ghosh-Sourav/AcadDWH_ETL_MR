package in.ac.iitkgp.acaddwh.service;

import java.util.List;

import in.ac.iitkgp.acaddwh.exception.*;

public interface ETLService<T> {
	
	public List<?> extract(String filePath, String splitter, String absoluteLogFileName) throws ExtractException;

	public int transform(List<?> items, String uniqueKeyFragment, String absoluteLogFileName) throws TransformException;

	public int load(List<?> items, String absoluteLogFileName) throws LoadException;
	
	public void warehouse(String hadoopLocalFileName, String absoluteLogFileName) throws WarehouseException;

}
