<jsp:include page="../commons/header.jsp" />
<jsp:include page="../guest/nav.jsp" />

<h2>
	<center>New Registration: Institute (Tenant)</center>
</h2>
<center>
	<form class="form-horizontal" role="form"
		action="/acaddwh/SignUpController" method="post">
		<table class="table table-hover info-details">
			<tbody>
				<tr>
					<th>Preferred Institute ID:</th>
					<td><input type="text" class="form-control"
						id="key" name="key" placeholder="Enter preferred ID"
						required="required"></td>
				</tr>
				<tr>
					<th>Institute Name:</th>
					<td colspan="2"><input type="text" class="form-control"
						id="name" name="name" placeholder="Enter name of the institute"
						required="required"></td>
				</tr>
				<tr>
					<th>Password:</th>
					<td colspan="2"><input type="password" class="form-control"
						id="password1" name="password" placeholder="Enter password"
						required="required"></td>
				</tr>
				<tr>
					<th>Confirm Password:</th>
					<td colspan="2"><input type="password" class="form-control"
						id="password2" name="password2" placeholder="Re-enter password"
						required="required"></td>
				</tr>
				<tr>
					<td colspan="4">
						<center>
							<input type="hidden" name="action" value="register_institute" />
							<button type="submit" class="btn btn-default">Sign up</button>
						</center>
					</td>
				</tr>
			</tbody>
		</table>
	</form>
	<br/><br/>
	<%
		if (request.getParameter("signUpFailed") != null) {
			out.println("<span class='alert alert-danger'>Institute sign up failed. Username might already exist.</span>");
		} else if (request.getParameter("signUpKey") != null) {
			out.println("<span class='alert alert-success'>Institute signed up with username <b>"
					+ request.getParameter("signUpKey") + "</b></span>");
		}
	%>
</center>

<jsp:include page="../commons/footer.jsp" />