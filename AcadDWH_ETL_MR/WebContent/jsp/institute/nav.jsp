
<%
	String url = request.getRequestURI();
%>
<jsp:include page="../commons/nav_top.jsp" />

<li><a href="#"><b>Welcome <%=session.getAttribute("displayName")%>
			(<%=session.getAttribute("key")%>)
	</b></a></li>
<li <%=url.endsWith("Home.jsp") ? "class='active'" : ""%>><a
	href="Home.jsp">Home</a></li>
<li><a href="/acaddwh/SignOutController"
	onclick="return confirm('This will sign you out. Are you sure?');">Sign
		out</a></li>

<jsp:include page="../commons/nav_bottom.jsp" />

