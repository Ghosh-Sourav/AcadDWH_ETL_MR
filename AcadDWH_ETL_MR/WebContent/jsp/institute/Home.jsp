<jsp:include page="../commons/header.jsp" />
<jsp:include page="nav.jsp" />
<jsp:include page="tasks.jsp" />

<h1>Welcome <%=session.getAttribute("displayName") %></h1>
<hr/>
 

<jsp:include page="../commons/footer.jsp" />