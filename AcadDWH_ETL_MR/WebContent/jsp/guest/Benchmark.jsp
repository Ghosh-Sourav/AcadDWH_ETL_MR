<%@page trimDirectiveWhitespaces="true"%>
<%@page import="in.ac.iitkgp.acaddwh.util.FileStats"%>
<%@page import="in.ac.iitkgp.acaddwh.bean.dim.Request"%>
<%@page import="java.util.List"%>
<%@page import="in.ac.iitkgp.acaddwh.bean.dim.Institute"%>
<%@page import="in.ac.iitkgp.acaddwh.service.impl.RequestServiceImpl"%>
<%@page import="in.ac.iitkgp.acaddwh.service.RequestService"%>
<%@page import="java.util.LinkedHashMap"%>
<%@page import="java.util.Map"%>institute_key,df,rows,size (in B),"split size (in B)","mappers (recorded)","mappers (estimated)","time, E&T MR task (ns)","time, E&T MR total task (ns)","time, E&T thread (ns)","time, L (ms)","time, effective ETL (ms)"<br/>
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
		String timeStats = etlRequest.getStatus()
				.replace("ETL Process completed successfully<br/> Split (in B): ", "")
				.replace("<br/> Mappers (Recorded): ", ",")
				.replace("<br/> Mappers (Estimated): ",",")
				.replace("<br/> E&T MR Max Task Time (ns): ", ",")
				.replace("<br/> E&T MR Total Task Time (ns): ",",")
				.replace("<br/> E&T Thread (ns): ", ",")
				.replace("<br/> L (ns): ", ",")
				.replace("<br/> Effective ETL (ns): ", ",")
				.replace("<br />", "");

		if (etlRequest.getStatus().contains("ETL Process completed successfully<br/>")) {
%><%=institute_key%>,<%=df%>,<%=rows%>,<%=size%>,<%=timeStats%><br/>
<%
	}
	}
%>