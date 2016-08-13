<%@page import="in.ac.iitkgp.acaddwh.config.ProjectInfo"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title><%=ProjectInfo.getWebsiteName()%></title>
<link rel="icon" href="/acaddwh/images/favicon.ico" type="image/x-icon">
<link rel="stylesheet" href="../../css/bootstrap.min.css" />
<link rel="stylesheet" href="../../css/bootstrap-theme.css" />
<link rel="stylesheet" href="../../css/styles.css" />
<link rel="stylesheet" href="../../css/jquery.dataTables.min.css">
<script src="../../js/jquery.min.js"></script>
<script type="text/javascript" src="../../js/jquery.dataTables.min.js"></script>
<script src="../../js/bootstrap.js"></script>
<script src="../../js/scripts.js"></script>
</head>
<body>
	<div class="container main-container">
		<br /> <br /> <br /> <br />
		<%
			if ((session == null || session.getAttribute("key") == null)
					&& !request.getRequestURI().startsWith("/acaddwh/jsp/guest/")) {
				System.out.println("Violation attempted");
				response.sendRedirect("/acaddwh/SignOutController");
		%>
		<h1>Unauthenticated access!</h1>
		<hr />
		<h3>
			Visit home page to <a href="/acaddwh/SignOutController">sign in</a> <br />
			<b>Possible Reason for this error:</b> You have been logged out!
		</h3>
	</div>
</body>
</html>
<%
	out.close();
	
	}
%>