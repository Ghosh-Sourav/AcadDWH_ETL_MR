package in.ac.iitkgp.acaddwh.controller;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import in.ac.iitkgp.acaddwh.service.AccountService;
import in.ac.iitkgp.acaddwh.service.impl.AccountServiceImpl;

/**
 * Servlet implementation class SignInController
 */
@WebServlet(urlPatterns = { "/SignInController" })
public class SignInController extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public SignInController() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String key = request.getParameter("key");
		String password = request.getParameter("password");

		AccountService accountService = new AccountServiceImpl();

		if (accountService.instituteExists(key, password)) {
			HttpSession session = request.getSession();
			session.setAttribute("key", key);
			session.setAttribute("displayName", accountService.getInstituteName(key));

			response.sendRedirect("jsp/institute/Home.jsp");
		} else {
			response.sendRedirect("jsp/guest/SignInPage.jsp?signInFailed=true");
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
