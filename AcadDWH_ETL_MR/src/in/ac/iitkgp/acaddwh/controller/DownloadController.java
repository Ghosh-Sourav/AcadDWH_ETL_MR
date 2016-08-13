package in.ac.iitkgp.acaddwh.controller;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import in.ac.iitkgp.acaddwh.config.ProjectInfo;

/**
 * Servlet implementation class DownloadController
 */
@WebServlet(asyncSupported = true, urlPatterns = { "/DownloadController" })
public class DownloadController extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public DownloadController() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		HttpSession session = request.getSession(false);

		if (session == null || session.getAttribute("key") == null) {
			response.sendRedirect("/acaddwh/SignOutController");

		} else {
			String instituteKey = session.getAttribute("key").toString();
			String fileName = request.getParameter("filename");
			String absoluteFileName = ProjectInfo.getUploadDirPath() + fileName;

			if (!instituteKey.equals(fileName.split("_")[1])) {
				response.sendRedirect("/acaddwh/SignOutController");

			} else {
				PrintWriter out = null;
				BufferedReader br = null;
				String line = null;
				try {
					out = response.getWriter();
					br = new BufferedReader(new FileReader(absoluteFileName));
					while ((line = br.readLine()) != null) {
						out.println(line);
					}
				} catch (Exception e) {
					System.out.println("File viewing failed for filename = " + fileName);
				} finally {
					if (br != null) {
						try {
							br.close();
						} catch (Exception e) {
						}
					}
					if (out != null) {
						try {
							out.close();
						} catch (Exception e) {
						}
					}
				}
			}

		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		doGet(request, response);
	}

}
