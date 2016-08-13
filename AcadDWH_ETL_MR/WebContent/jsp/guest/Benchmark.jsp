<%@page trimDirectiveWhitespaces="true"%>
<%@page import="in.ac.iitkgp.acaddwh.util.FileStats"%>
<%@page import="in.ac.iitkgp.acaddwh.bean.dim.Request"%>
<%@page import="java.util.List"%>
<%@page import="in.ac.iitkgp.acaddwh.bean.dim.Institute"%>
<%@page import="in.ac.iitkgp.acaddwh.service.impl.RequestServiceImpl"%>
<%@page import="in.ac.iitkgp.acaddwh.service.RequestService"%>
<%@page import="java.util.LinkedHashMap"%>
<%@page import="java.util.Map"%>institute_key,df,rows,size (in B),"time, E (ms)","time, T (ms)","time, L/W (ms)","time, ETL/W (ms)"<br/>
<%
	RequestService requestService = new RequestServiceImpl();
	List<Request> etlRequests = requestService.getLogs();

	for (Request etlRequest : etlRequests) {
		String institute_key = etlRequest.getFileNameWithoutExtn()
				.substring(etlRequest.getFileNameWithoutExtn().indexOf("_") + 1).split("_")[0];
		String df = etlRequest.getFileNameWithoutExtn()
				.substring(etlRequest.getFileNameWithoutExtn().indexOf("_") + 1)
				.replace(institute_key + "_", "");
		long rows = FileStats.getLineCount(etlRequest.getFileNameWithoutExtn() + ".csv");
		long size = FileStats.getSizeInBytes(etlRequest.getFileNameWithoutExtn() + ".csv");
		String timeStats = etlRequest.getStatus().replace("ETL Process completed successfully<br/> E: ", "")
				.replace("<br/> T: ", ",").replace("<br/> L/W: ", ",").replace("<br/> ETL/W: ", ",")
				.replace("<br />", "");

		if (etlRequest.getStatus().contains("ETL Process completed successfully<br/> E: ")) {
%><%=institute_key%>,<%=df%>,<%=rows%>,<%=size%>,<%=timeStats%><br/>
<%
	}
	}
%>