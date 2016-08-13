<center>

<div class="dropdown">
  <button class="dropbtn">Data Warehousing</button>
  <div class="dropdown-content">
    <a href="ETL.jsp">ETL Processes</a>
    <a href="RequestList.jsp">Track Requests</a>
  </div>
</div>

<div class="dropdown">
  <button class="dropbtn">Reports</button>
  <div class="dropdown-content">
    <a href="#">...</a>
  </div>
</div>


</center>
<hr/>

<%
	String msg = (String)session.getAttribute("msg");
	String msgClass = (String)session.getAttribute("msgClass");

	if (msg != null && !"null".equals(msg) && msgClass != null && !"null".equals(msgClass)) {
%>
<center>
	<div class="alert alert-success <%=msgClass%> fade in">
		<a class="close" data-dismiss="alert" aria-label="close">&times;</a>
		<%=msg%>
	</div>
</center>
<%
		session.removeAttribute("msg");
		session.removeAttribute("msgClass");
	}
%>