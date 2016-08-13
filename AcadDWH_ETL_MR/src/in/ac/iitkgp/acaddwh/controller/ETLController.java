package in.ac.iitkgp.acaddwh.controller;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.Part;

import org.apache.tomcat.util.http.fileupload.servlet.ServletFileUpload;

import in.ac.iitkgp.acaddwh.background.ETLDriver;
import in.ac.iitkgp.acaddwh.bean.dim.Request;
import in.ac.iitkgp.acaddwh.config.ProjectInfo;
import in.ac.iitkgp.acaddwh.service.RequestService;
import in.ac.iitkgp.acaddwh.service.impl.RequestServiceImpl;
import in.ac.iitkgp.acaddwh.util.KeyRepository;

/**
 * Servlet implementation class ETLController
 */
@WebServlet(asyncSupported = true, urlPatterns = { "/ETLController" })
@MultipartConfig(location = "/home/mt1/15CS60R16/AcadDWH/tempUpload", fileSizeThreshold = 2000000000, maxFileSize = -1, maxRequestSize = -1) /*
																																								 * Sizes
																																								 * in
																																								 * B
																																								 */
public class ETLController extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public ETLController() {
		super();
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		HttpSession session = request.getSession(false);

		if (session == null || session.getAttribute("key") == null || !ServletFileUpload.isMultipartContent(request)) {
			response.sendRedirect("/acaddwh/SignOutController");

		} else {

			try {
				String requestKey = KeyRepository.getKey();
				String instituteKey = session.getAttribute("key").toString();
				String df = request.getParameter("df");
				String savePath = ProjectInfo.getUploadDirPath();
				String fileNameWithoutExtn = requestKey + "_" + instituteKey + "_" + df;
				String absoluteFileNameWithoutExtn = savePath + fileNameWithoutExtn;

				System.out.println("File to be saved as " + absoluteFileNameWithoutExtn + ".csv");

				File fileSaveDir = new File(savePath);
				if (!fileSaveDir.exists()) {
					fileSaveDir.mkdir();
				}

				Collection<Part> parts = request.getParts();

				Request etlRequest = new Request();
				etlRequest.setRequestKey(requestKey);
				etlRequest.setInstituteKey(instituteKey);
				etlRequest.setFileNameWithoutExtn(fileNameWithoutExtn);
				etlRequest.setStatus("File uploading...");

				RequestService requestService = new RequestServiceImpl();
				if (requestService.addLog(etlRequest) == 0) {
					throw (new Exception());
				}

				Runnable runnable = new ETLDriver(etlRequest, df, absoluteFileNameWithoutExtn, parts);
				Thread etlDriverThread = new Thread(runnable);
				etlDriverThread.start();

				session.setAttribute("msg", "ETL Process Initiated with Request ID " + requestKey);
				session.setAttribute("msgClass", "alert-success");
			} catch (Exception e) {
				e.printStackTrace();
				session.setAttribute("msg", "ETL Process could not be initiated.");
				session.setAttribute("msgClass", "alert-success");
			}

			response.sendRedirect("jsp/institute/ETL.jsp");
		}
	}

}
