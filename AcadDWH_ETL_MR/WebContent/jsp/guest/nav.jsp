<% String url = request.getRequestURI(); %>
<jsp:include page="../commons/nav_top.jsp"/>

<li <%=url.endsWith("SignInPage.jsp")?"class='active'":""%>><a href="SignInPage.jsp">Sign in</a></li>
<li <%=url.endsWith("SignUpPage.jsp")?"class='active'":""%>><a href="SignUpPage.jsp">Sign up</a></li>

<jsp:include page="../commons/nav_bottom.jsp"/>	
	