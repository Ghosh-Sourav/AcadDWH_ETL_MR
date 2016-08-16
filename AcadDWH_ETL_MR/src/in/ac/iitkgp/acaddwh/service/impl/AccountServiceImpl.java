package in.ac.iitkgp.acaddwh.service.impl;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import in.ac.iitkgp.acaddwh.bean.dim.Institute;
import in.ac.iitkgp.acaddwh.config.HadoopNodeInfo;
import in.ac.iitkgp.acaddwh.config.NameNodeInfo;
import in.ac.iitkgp.acaddwh.config.ProjectInfo;
import in.ac.iitkgp.acaddwh.dao.dim.AccountDAO;
import in.ac.iitkgp.acaddwh.dso.ItemDSO;
import in.ac.iitkgp.acaddwh.exception.ETLException;
import in.ac.iitkgp.acaddwh.service.AccountService;
import in.ac.iitkgp.acaddwh.service.ETLService;
import in.ac.iitkgp.acaddwh.service.etl.dim.InstituteETL;
import in.ac.iitkgp.acaddwh.util.Cryptography;
import in.ac.iitkgp.acaddwh.util.DBConnection;
import in.ac.iitkgp.acaddwh.util.HdfsManager;

public class AccountServiceImpl implements AccountService {

	@Override
	public boolean instituteExists(String key, String password) {
		boolean exists = false;
		password = Cryptography.encrypt(key + password);
		Institute institute = null;

		Connection con = DBConnection.getReadConnection();
		AccountDAO accountDAO = new AccountDAO();

		try {
			institute = accountDAO.getInstitute(con, key, password);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			DBConnection.closeConnection(con);
		}

		if (institute != null) {
			exists = true;
		}
		return exists;
	}

	@Override
	public String getInstituteName(String key) {
		String instituteName = "";

		Connection con = DBConnection.getReadConnection();
		AccountDAO accountDAO = new AccountDAO();

		try {
			instituteName = accountDAO.getInstitute(con, key).getInstituteName();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			DBConnection.closeConnection(con);
		}

		return instituteName;
	}

	@Override
	public String signUpInstitute(Institute institute) throws ETLException {
		
		String savePath = ProjectInfo.getUploadDirPath();
		String fileNameWithoutExtn = "000" + "_" + institute.getInstituteKey() + "_" + "ac";
		String absoluteFileNameWithoutExtn = savePath + fileNameWithoutExtn;

		List<Institute> institutes = new ArrayList<Institute>();
		institutes.add(institute);

		ETLService<Institute> etlService = new InstituteETL();
		try {
			String shortFileName = new File(absoluteFileNameWithoutExtn + ".csv").getName();
			
			((InstituteETL) etlService).transform(institutes, null, null);
			((InstituteETL) etlService).loadToDB(institutes, null);
			
			ItemDSO.writeTransformedCSV(institutes, absoluteFileNameWithoutExtn + ".csv");
			HdfsManager.copyFileToHdfs(absoluteFileNameWithoutExtn + ".csv");

			String hdfsInstituteFilePath = NameNodeInfo.getUrl()
					+ HadoopNodeInfo.getPathInHdfs() + shortFileName;
			etlService.load(hdfsInstituteFilePath, absoluteFileNameWithoutExtn + "-report.txt");
			
		} catch (ETLException e) {
			throw (e);
		} catch(IOException e) {
			throw (new ETLException());
		}

		return institute.getInstituteKey();
	}

}
