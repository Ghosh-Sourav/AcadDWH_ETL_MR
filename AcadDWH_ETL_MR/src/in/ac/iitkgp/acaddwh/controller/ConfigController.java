package in.ac.iitkgp.acaddwh.controller;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import in.ac.iitkgp.acaddwh.config.HadoopNodeInfo;
import in.ac.iitkgp.acaddwh.config.HadoopNodeInfo.MapCount;

/**
 * Servlet implementation class ConfigController
 */
@WebServlet("/ConfigController")
public class ConfigController extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public ConfigController() {
		super();
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

			String key = request.getParameter("key");
			System.out.println("Key to update = " + key);

			if ("noOfMappers".equals(key)) {
				String noOfMappers = request.getParameter("noOfMappers");
				System.out.println("New Value = " + noOfMappers);

				if ("1".equals(noOfMappers)) {
					HadoopNodeInfo.setNoOfMappersRequired(MapCount.ONE_MAPPER);
				} else if ("2".equals(noOfMappers)) {
					HadoopNodeInfo.setNoOfMappersRequired(MapCount.TWO_MAPPERS);
				} else if ("proportional".equals(noOfMappers)) {
					HadoopNodeInfo.setNoOfMappersRequired(MapCount.PROPORTIONAL_TO_FILESIZE);
				}
			}

			session.setAttribute("msg", "Global configuration updated");
			session.setAttribute("msgClass", "alert-info");

			response.sendRedirect("jsp/institute/ETL.jsp");

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
