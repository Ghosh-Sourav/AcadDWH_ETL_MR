
$(document).ready(function(){
	$("a[href='#']").click(function(){
		return false;
	});
	
	//$('.list-table').dataTable();
	$('.list-table').dataTable( {
        "order": [[ 0, "desc" ]]
    } );
	
	
	if ($('#password1').length == 1) {
		
		$('#password1').attr("pattern", "(?=.*[0-9])(?=.*[a-z])(?=.*[A-Z]).{8,}");
		$('#password1').attr("title", "Password must contain at least one lowercase letter, one uppercase letter, and be of at least 8 characters");
		$('#password1').change(function(){
			$('#password2').attr("pattern", $('#password1')[0].value);
		});
		
		$('#password2').attr("title", "Passwords do not match");
		
		$('#key').change(function(){
			$('#key')[0].value = $('#key')[0].value.toUpperCase().replace(/[^A-Z0-9]/g,'');
		});
		
	}
	
	if ($('.fileUploadForm').length > 0) {
		
		$('.fileUploadForm').submit(function(){
			var thisForm = $(this);

			$('.uploadButton').attr("disabled", "disabled");
			$('.uploadButton').removeClass("btn-info");
			$('.fileInput').attr("disabled", "disabled");
		
			thisForm.find('.fileInput').removeAttr("disabled");
			thisForm.find('.uploadButton').addClass("btn-info");
			thisForm.find('.uploadButton')[0].innerHTML = "Uploading...";
	
		});
		
	}
});