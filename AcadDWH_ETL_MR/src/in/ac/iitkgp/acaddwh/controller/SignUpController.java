package in.ac.iitkgp.acaddwh.controller;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import in.ac.iitkgp.acaddwh.bean.dim.Institute;
import in.ac.iitkgp.acaddwh.service.AccountService;
import in.ac.iitkgp.acaddwh.service.impl.AccountServiceImpl;

/**
 * Servlet implementation class SignUpController
 */
@WebServlet("/SignUpController")
public class SignUpController extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public SignUpController() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String instituteKey = request.getParameter("key");
		String instituteName = request.getParameter("name");
		String institutePassword = request.getParameter("password");

		AccountService accountService = new AccountServiceImpl();
		Institute institute = new Institute();
		institute.setInstituteKey(instituteKey);
		institute.setInstituteName(instituteName);
		institute.setInstitutePassword(institutePassword);

		try {
			instituteKey = accountService.signUpInstitute(institute);
			System.out.println("Sign up successful!");
			response.sendRedirect("jsp/guest/SignUpPage.jsp?signUpKey=" + instituteKey);
		} catch (Exception e) {
			response.sendRedirect("jsp/guest/SignUpPage.jsp?signUpFailed=true");
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
