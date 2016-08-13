package in.ac.iitkgp.acaddwh;

import java.util.List;

import in.ac.iitkgp.acaddwh.config.ProjectInfo;
import in.ac.iitkgp.acaddwh.exception.ETLException;
import in.ac.iitkgp.acaddwh.service.etl.dim.StudentETL;

public class Driver {

	

	public void driveDimStudents(String instituteCode) {

		int resultCount = 0;
		StudentETL studentETL = new StudentETL();
		
		try {
			List<?> students = studentETL.extract(ProjectInfo.getUploadDirPath() + "IITKGP_027412734_dim_students.csv", ",", null);

			System.out.println("Extracted " + students.size() + " items");

			resultCount = studentETL.transform(students, instituteCode, null);
			System.out.println("Transformed " + resultCount + " items");

			resultCount = studentETL.load(students, null);
			System.out.println("Loaded " + resultCount + " items");
			
		} catch (ETLException e) {
			e.printStackTrace();
		}

	}

	public void driveTeacher(String instituteCode) {

	}

	public static void main(String[] args) {
		Driver driver = new Driver();

		String instituteCode = "IITKGP_027412734";
		driver.driveDimStudents(instituteCode);
	}

}
