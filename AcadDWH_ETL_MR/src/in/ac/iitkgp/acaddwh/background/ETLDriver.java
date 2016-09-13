package in.ac.iitkgp.acaddwh.background;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import javax.servlet.http.Part;

import in.ac.iitkgp.acaddwh.bean.dim.Request;
import in.ac.iitkgp.acaddwh.config.HadoopNodeInfo;
import in.ac.iitkgp.acaddwh.config.NameNodeInfo;
import in.ac.iitkgp.acaddwh.exception.ETLException;
import in.ac.iitkgp.acaddwh.exception.ExtractAndTransformException;
import in.ac.iitkgp.acaddwh.exception.ExtractException;
import in.ac.iitkgp.acaddwh.exception.LoadException;
import in.ac.iitkgp.acaddwh.service.ETLService;
import in.ac.iitkgp.acaddwh.service.RequestService;
import in.ac.iitkgp.acaddwh.service.etl.ETLHelper;
import in.ac.iitkgp.acaddwh.service.etl.dim.*;
import in.ac.iitkgp.acaddwh.service.etl.fact.*;
import in.ac.iitkgp.acaddwh.service.impl.RequestServiceImpl;
import in.ac.iitkgp.acaddwh.util.FileStats;
import in.ac.iitkgp.acaddwh.util.HdfsManager;

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

		// TODO: This FOR loop is for evaluating; needs to be removed
		/* EVAL CODE: BEGIN */
		String originalRequestKey = request.getRequestKey();
		String originalAbsoluteFileNameWithoutExtn = absoluteFileNameWithoutExtn;
		for (int i = 0; i < 99; i++) {
			Path srcPath = new File(originalAbsoluteFileNameWithoutExtn + ".csv").toPath();
			Path destPath = new File(originalAbsoluteFileNameWithoutExtn + "_" + i + ".csv").toPath();
			absoluteFileNameWithoutExtn = originalAbsoluteFileNameWithoutExtn + "_" + i;
			try {
				Files.copy(srcPath, destPath);
				request.setRequestKey(originalRequestKey + "_" + i);
				drive(etlClass);
				deleteFile(absoluteFileNameWithoutExtn + ".csv");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		absoluteFileNameWithoutExtn = originalAbsoluteFileNameWithoutExtn;
		/* EVAL CODE: END */
		
		deleteFile(absoluteFileNameWithoutExtn + ".csv");
		
	}

	private void drive(Class<?> etlClass)
			throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		long timeInitial, timePostExtractAndTransform, timePostLoad;
		String shortFileName;

		ETLService<?> etlService = (ETLService<?>) etlClass.newInstance();

		try {

			HdfsManager.copyFileToHdfs(absoluteFileNameWithoutExtn + ".csv");

			shortFileName = new File(absoluteFileNameWithoutExtn + ".csv").getName();

			request.setStatus(
					"Extracting and Transforming..." + "<br/> Split: " + HadoopNodeInfo.getSplitSize(shortFileName));
			requestService.updateLog(request);

			timeInitial = ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId());

			boolean extractAndTransformReturnValue = etlService.extractAndTransform(shortFileName,
					request.getInstituteKey(), absoluteFileNameWithoutExtn + "-report.txt");
			timePostExtractAndTransform = ManagementFactory.getThreadMXBean()
					.getThreadCpuTime(Thread.currentThread().getId());
			if (!extractAndTransformReturnValue) {
				System.out.println("[" + shortFileName + "]: ExtractAndTransformException thrown!");
				throw (new ExtractAndTransformException());
			}
			System.out.println("[" + shortFileName + "]: Extracted and Transformed!");
			request.setStatus("Extraction and Transformation completed, Loading..." + "<br/> Split: "
					+ HadoopNodeInfo.getSplitSize(shortFileName) + "<br/> E&T: "
					+ (timePostExtractAndTransform - timeInitial));
			requestService.updateLog(request);

			List<String> partFilePaths = HdfsManager.getPartFilePaths(NameNodeInfo.getUrl()
					+ HadoopNodeInfo.getPathInHdfs() + "outputDir_" + shortFileName.replace(".", "_"));
			for (String partFilePath : partFilePaths) {
				//etlService.load(partFilePath, absoluteFileNameWithoutExtn + "-report.txt");
			}
			timePostLoad = ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId());
			System.out.println("[" + shortFileName + "]: Loaded!");

			Long etMaxTaskTime = ETLHelper.getMaxTimeTakenInfo("extractAndTransform_" + shortFileName);
			Long etTaskTotalTime = ETLHelper.getTotalTimeTakenInfo("extractAndTransform_" + shortFileName);
			ETLHelper.removeMaxTimeTakenInfo("extractAndTransform_" + shortFileName);
			long effectiveETLTime = (timePostLoad - timePostExtractAndTransform) + etMaxTaskTime;

			System.out.println("Size of " + (absoluteFileNameWithoutExtn + ".csv") + " is "
					+ (FileStats.getSizeInBytes(shortFileName)));

			request.setStatus("ETL Process completed successfully" + "<br/> Rows: "
					+ FileStats.getLineCount(shortFileName) + "<br/> Input File Size (in B): "
							+ FileStats.getSizeInBytes(shortFileName) + "<br/> Split (in B): "
					+ HadoopNodeInfo.getSplitSize(shortFileName) + "<br/> Mappers (Recorded): "
					+ ((!HadoopNodeInfo.isReducerToBeUsed()) ? partFilePaths.size() : "-1")
					+ "<br/> Mappers (Estimated): "
					+ Math.ceil((double) FileStats.getSizeInBytes(shortFileName)
							/ (double) HadoopNodeInfo.getSplitSize(shortFileName))
					+ "<br/> E&T MR Max Task Time (ns): " + etMaxTaskTime + "<br/> E&T MR Total Task Time (ns): "
					+ etTaskTotalTime + "<br/> E&T Thread (ns): " + (timePostExtractAndTransform - timeInitial)
					+ "<br/> L (ns): " + (timePostLoad - timePostExtractAndTransform) + "<br/> Effective ETL (ns): "
					+ effectiveETLTime);
			requestService.updateLog(request);

		} catch (ExtractAndTransformException e) {
			System.out.println("Extraction/Transformation failed!");
			request.setStatus("Extraction/Transformation failed, ETL Aborted");
			requestService.updateLog(request);

		} catch (LoadException e) {
			System.out.println("Loading failed!");
			request.setStatus("Loading failed, ETL Aborted");
			requestService.updateLog(request);

		} catch (Exception e) {
			System.out.println("Exeption occurred!");
			request.setStatus(request.getStatus() + " Aborted!");
			requestService.updateLog(request);

		}
	}

}
