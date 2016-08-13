<%@page import="in.ac.iitkgp.acaddwh.util.FileStats"%>
<%@page import="in.ac.iitkgp.acaddwh.bean.dim.Request"%>
<%@page import="java.util.List"%>
<%@page import="in.ac.iitkgp.acaddwh.bean.dim.Institute"%>
<%@page import="in.ac.iitkgp.acaddwh.service.impl.RequestServiceImpl"%>
<%@page import="in.ac.iitkgp.acaddwh.service.RequestService"%>
<%@page import="java.util.LinkedHashMap"%>
<%@page import="java.util.Map"%>
<jsp:include page="../commons/header.jsp" />
<jsp:include page="nav.jsp" />
<jsp:include page="tasks.jsp" />
<%
	String instituteKey = (String) session.getAttribute("key");
	Institute institute = new Institute();
	institute.setInstituteKey(instituteKey);

	RequestService requestService = new RequestServiceImpl();
	List<Request> etlRequests = requestService.getLogs(institute);
%>
<h1>Track Requests</h1>
<hr />

<table class="table table-hover list-table" style="text-align: center;">
	<thead>
		<tr>
			<th style="text-align: center;">Request ID</th>
			<th style="text-align: center;">Uploaded CSV</th>
			<th style="text-align: center;"></th>
			<th style="text-align: center;">Status</th>
			<th style="text-align: center;">
				Error Report
				(
				<a href="/acaddwh/csv/header_log_request.csv" target="_blank">
					<span style="font-weight: lighter;">Download Header</span>
				</a>
				)
			</th>
		</tr>
	</thead>
	<tbody>
		<%
			for (Request etlRequest : etlRequests) {
		%>
		<tr class="list-items">
			<td><%=etlRequest.getRequestKey()%></td>
			<td><a href="/acaddwh/DownloadController?filename=<%=etlRequest.getFileNameWithoutExtn()%>.csv" target="_blank">View CSV file</a></td>
			<td>
				<%=etlRequest.getFileNameWithoutExtn().substring(etlRequest.getFileNameWithoutExtn().indexOf("_")+1)%><br/>
				Rows: <%=FileStats.getLineCount(etlRequest.getFileNameWithoutExtn()+".csv") %><br/>
				Size (in B): <%=FileStats.getSizeInBytes(etlRequest.getFileNameWithoutExtn()+".csv") %>
			</td>
			<td><%=etlRequest.getStatus() %></td>
			<td>
				<%
					if (etlRequest.getStatus().contains("failed")) {
						%>
						<a href="/acaddwh/DownloadController?filename=<%=etlRequest.getFileNameWithoutExtn()%>-report.txt" target="_blank">View error report</a>
						<%
					} else if (etlRequest.getStatus().contains("Aborted!")) {
						%>
						Contact service provider for further information
						<%
					} else {
						out.println("N/A");
					}
				%>
			</td>
		</tr>
		<%
			}
		%>
	</tbody>
</table>

<jsp:include page="../commons/footer.jsp" />