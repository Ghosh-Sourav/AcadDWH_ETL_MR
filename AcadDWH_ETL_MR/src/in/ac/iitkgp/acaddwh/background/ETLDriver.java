package in.ac.iitkgp.acaddwh.background;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.List;

import javax.servlet.http.Part;

import in.ac.iitkgp.acaddwh.bean.dim.Request;
import in.ac.iitkgp.acaddwh.config.ProjectInfo;
import in.ac.iitkgp.acaddwh.dso.ItemDSO;
import in.ac.iitkgp.acaddwh.exception.ETLException;
import in.ac.iitkgp.acaddwh.exception.ExtractException;
import in.ac.iitkgp.acaddwh.exception.WarehouseException;
import in.ac.iitkgp.acaddwh.exception.LoadException;
import in.ac.iitkgp.acaddwh.exception.TransformException;
import in.ac.iitkgp.acaddwh.service.ETLService;
import in.ac.iitkgp.acaddwh.service.RequestService;
import in.ac.iitkgp.acaddwh.service.etl.dim.*;
import in.ac.iitkgp.acaddwh.service.etl.fact.*;
import in.ac.iitkgp.acaddwh.service.impl.RequestServiceImpl;
import in.ac.iitkgp.acaddwh.util.SCP;

public class ETLDriver implements Runnable {

	private Request request = null;
	private String df = null;
	private String absoluteFileNameWithoutExtn = null;
	private Collection<Part> parts = null;

	RequestService requestService = null;

	public ETLDriver(Request request, String df, String absoluteFileNameWithoutExtn, Collection<Part> parts) {
		this.request = request;
		this.df = df;
		this.absoluteFileNameWithoutExtn = absoluteFileNameWithoutExtn;
		this.parts = parts;

		requestService = new RequestServiceImpl();
	}

	@Override
	public void run() {
		try {
			// Thread.sleep(20000);

			uploadCsvFile();
			if (new File(absoluteFileNameWithoutExtn + ".csv").exists()) {
				System.out.println("File saved as " + absoluteFileNameWithoutExtn + ".csv");
				request.setStatus("File upload completed, Extracting...");
				requestService.updateLog(request);
			}

			System.out.println("Inititating ETL [df = " + df + ", instituteKey = " + request.getInstituteKey() + "]");
			processETL();

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("ETLDriver thread aborted!");
			request.setStatus(request.getStatus() + " Aborted!");
			requestService.updateLog(request);
		} finally {
			// deleteFile();
		}
	}

	private void uploadCsvFile() throws IOException {
		for (Part part : parts) {
			part.write(absoluteFileNameWithoutExtn + ".csv");
		}
	}

	@SuppressWarnings("unused")
	private boolean deleteFile(String fileName) {
		return new File(fileName).delete();
	}

	private void processETL()
			throws ClassNotFoundException, InstantiationException, IllegalAccessException, ETLException {
		Class<?> etlClass = null;

		switch (df) {
		case "dim_departments":
			etlClass = DepartmentETL.class;
			break;
		case "dim_specialisations":
			etlClass = SpecialisationETL.class;
			break;
		case "dim_students":
			etlClass = StudentETL.class;
			break;
		case "dim_teachers":
			etlClass = TeacherETL.class;
			break;
		case "dim_courses":
			etlClass = CourseETL.class;
			break;
		case "dim_eval_areas":
			etlClass = EvalAreaETL.class;
			break;
		case "dim_regtypes":
			etlClass = RegtypeETL.class;
			break;
		case "dim_times":
			etlClass = TimeETL.class;
			break;

		case "fact_sem_performance":
			etlClass = SemPerformanceETL.class;
			break;
		case "fact_spl_performance":
			etlClass = SplPerformanceETL.class;
			break;
		case "fact_stu_learning":
			etlClass = StuLearningETL.class;
			break;
		case "fact_teaching_quality":
			etlClass = TeachingQualityETL.class;
			break;

		default:
			throw (new ExtractException());
		}
		drive(etlClass);
	}

	private void drive(Class<?> etlClass)
			throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		int resultCount = 0;
		long timeInitial, timePostExtract, timePostTransform, timePostWarehouse;

		ETLService<?> etlService = (ETLService<?>) etlClass.newInstance();

		try {
			timeInitial = ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId())
					/ 1000000;

			List<?> items = etlService.extract(absoluteFileNameWithoutExtn + ".csv", ",",
					absoluteFileNameWithoutExtn + "-report.txt");
			timePostExtract = ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId())
					/ 1000000;
			System.out.println("Extracted " + items.size() + " items");
			request.setStatus("Extraction completed, Transforming..." + "<br/> E: " + (timePostExtract - timeInitial));
			requestService.updateLog(request);

			resultCount = etlService.transform(items, request.getInstituteKey(),
					absoluteFileNameWithoutExtn + "-report.txt");
			timePostTransform = ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId())
					/ 1000000;
			System.out.println("Transformed " + resultCount + " items");
			request.setStatus("Transformation completed, Loading..." + "<br/> E: " + (timePostExtract - timeInitial)
					+ "<br/> T: " + (timePostTransform - timePostExtract));
			requestService.updateLog(request);

			if (ProjectInfo.isConstraintViolationReqd()) {
				System.out.println("Constraint validation required; Loading into DB before warehousing...");
				resultCount = etlService.load(items, absoluteFileNameWithoutExtn + "-report.txt");
				System.out.println("Loaded " + resultCount + " items");
				request.setStatus("Loading completed with constraint checking, Warehousing..." + "<br/> E: "
						+ (timePostExtract - timeInitial) + "<br/> T: " + (timePostTransform - timePostExtract));
				requestService.updateLog(request);
			} else {
				System.out.println("Constraint validation not required; Skipping loading into DB phase...");
				request.setStatus("Transformation completed, Warehousing..." + "<br/> E: "
						+ (timePostExtract - timeInitial) + "<br/> T: " + (timePostTransform - timePostExtract));
				requestService.updateLog(request);
			}

			/*
			 * Save warehoused output to "-hive.csv" file, and send to hadoop
			 * node
			 */
			ItemDSO.writeTransformedCSV(items, absoluteFileNameWithoutExtn + "-hive.csv");
			String hadoopLocalFileName = SCP.sendToHadoopNode(absoluteFileNameWithoutExtn + "-hive.csv");

			etlService.warehouse(hadoopLocalFileName, absoluteFileNameWithoutExtn + "-report.txt");
			timePostWarehouse = ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId())
					/ 1000000;
			System.out.println("Warehoused " + hadoopLocalFileName);
			request.setStatus("ETL Process completed successfully" + "<br/> E: " + (timePostExtract - timeInitial)
					+ "<br/> T: " + (timePostTransform - timePostExtract) + "<br/> L/W: "
					+ (timePostWarehouse - timePostTransform) + "<br/> ETL/W: " + (timePostWarehouse - timeInitial));
			requestService.updateLog(request);

		} catch (ExtractException e) {
			System.out.println("Extraction failed!");
			request.setStatus("Extraction failed, ETL Aborted");
			requestService.updateLog(request);

		} catch (TransformException e) {
			System.out.println("Transformation failed!");
			request.setStatus("Transformation failed, ETL Aborted");
			requestService.updateLog(request);

		} catch (LoadException e) {
			System.out.println("Loading failed!");
			request.setStatus("Loading failed, ETL Aborted");
			requestService.updateLog(request);

		} catch (WarehouseException e) {
			System.out.println("Warehousing failed!");
			request.setStatus("Warehousing failed, ETL completed upto Loading phase");
			requestService.updateLog(request);

		} catch (Exception e) {
			System.out.println("Exeption occurred!");
			request.setStatus(request.getStatus() + " Aborted!");
			requestService.updateLog(request);

		}
	}

}
