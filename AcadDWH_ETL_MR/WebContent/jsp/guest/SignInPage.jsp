<jsp:include page="../commons/header.jsp" />
<jsp:include page="../guest/nav.jsp" />
<div class="row">
	<div class="col-md-6">
	<br/>
		<div class="banner-image" style="text-align: center;">
			<img src="../../images/banner.jpg" width="560px" style="border-radius: 4px; box-shadow: #010101"/>
		</div>
	</div>
	<div class="col-md-6">
		<br /> <br /> <br /> <br /> <br /> <br />		
		<div style="text-align: center;">
			<form class="form-horizontal" role="form"
				action="/acaddwh/SignInController" method="post">
				<div class="form-group">
					<label class="control-label col-sm-3" for="username">Institute ID:</label>
					<div class="col-sm-9">
						<input type="text" class="form-control" id="key"
							name="key" placeholder="Enter username" required="required" autofocus="autofocus">
					</div>
				</div>
				<div class="form-group">
					<label class="control-label col-sm-3" for="password">Password:</label>
					<div class="col-sm-9">
						<input type="password" class="form-control" id="password"
							name="password" placeholder="Enter password" required="required">
					</div>
				</div>				
				<div class="form-group">
					<div class="col-sm-offset-3 col-sm-9">
						<button type="submit" id="btnSignIn" class="btn btn-success">Sign in</button>
					</div>
				</div>
				<div class="form-group"></div>
				<div class="form-group">
					<div class="col-sm-offset-3 col-sm-9">
						<%
							if (request.getParameter("signInFailed") != null) {
								out.println("<span class='alert alert-danger'>Incorrect credentials provided.</span>");
							} else if (request.getParameter("signedOut") != null) {
								out.println("<span class='alert alert-success'>You have been logged out.</span>");
							} 
						%>
					</div>
				</div>
			</form>
		</div>
	</div>
</div>


<jsp:include page="../commons/footer.jsp" />