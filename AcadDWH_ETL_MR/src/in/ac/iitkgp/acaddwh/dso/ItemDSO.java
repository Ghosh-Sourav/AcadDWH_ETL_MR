package in.ac.iitkgp.acaddwh.dso;

import java.util.List;

import in.ac.iitkgp.acaddwh.bean.Item;
import in.ac.iitkgp.acaddwh.exception.WarehouseException;
import in.ac.iitkgp.acaddwh.util.DataWriter;

public class ItemDSO {
	
	@SuppressWarnings("unchecked")
	public static void writeTransformedCSV(List<?> items, String filePath) throws WarehouseException {

		StringBuffer content = new StringBuffer();
		DataWriter.writeToFile(filePath, "");

		try {
			for (Item item : (List<Item>) items) {
				content.append(item.getPrintableLine());

				if (content.length() > 10000) {
					DataWriter.appendToFile(filePath, content.toString());
					content.delete(0, content.length());
				}
			}

			DataWriter.appendToFile(filePath, content.toString());
			content.delete(0, content.length());

		} catch (Exception e) {
			throw (new WarehouseException());
		}
		
		System.out.println("Transformed items saved as: "+filePath);
	}
}
